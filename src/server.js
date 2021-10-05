const SerialPort = require('serialport');
const Readline = require('@serialport/parser-readline')
const {baudRate, fractionDigits, mqttUrl, serial, deviceMapping, discoveryPrefix, identifier, mqttUser, mqttPassword, invertNegativeValues, sensorValueThreshold} = require('./config');
const mqtt = require('mqtt');
const log = require('./log');

// MQTT Connection
const mqttOptions = {};
if (mqttUser) {
    mqttOptions.username = mqttUser;
}
if (mqttPassword) {
    mqttOptions.password = mqttPassword;
}
log.info(`Connecting to MQTT broker [${mqttUrl}]...`);
log.debug(`MQTT options [${JSON.stringify(mqttOptions)}]`);
const mqttClient  = mqtt.connect(mqttUrl, mqttOptions);

// HA sensors initial creation
mqttClient.on('connect', function () {
    log.info(`Connected to MQTT broker [${mqttUrl}].`);
    createHASensors();
})

// Device mapping loading
log.info(`Loading device mapping [${deviceMapping}]...`);
const deviceMappingJson = require(`./device-mapping/${deviceMapping}`);
log.info(`Loaded device mapping [${deviceMapping}]`);
log.debug(`Device mapping contents [${JSON.stringify(deviceMappingJson)}]`);

// Serial port opening
const serialPort = new SerialPort(serial, {
    baudRate: baudRate,
    autoOpen: false
});
log.info(`Opening serial port [${serial}]...`);
serialPort.open(function (err) {
    if (err) {
        log.error(`Error opening serial port [${serial}] : `, err.message);
        process.exit(1);
    }
});
log.info(`Opened serial port [${serial}]`);

// Serial port data processing
const parser = new Readline({ delimiter: '\n' });
serialPort.pipe(parser);
let receivedSerialData = false;
let lastDataReceived = 0;
parser.on('data', function (data) {

    // Example of values: http://lechacal.com/wiki/index.php?title=RPICT7V1_v2.0
    //     NodeID  RP1     RP2     RP3     RP4     RP5     RP6     RP7     Irms1   Irms2   Irms3   Irms4   Irms5   Irms6   Irms7   Vrms
    //     11      0.0     0.0     0.0     -0.0    0.0     0.0     -0.0    202.1   208.6   235.3   207.2   223.4   3296.3  2310.8  0.9

    // Values from sensor are returned with space/tab between each value.
    const values = data.split(/[ ,]+/);
    let count = 0;
    lastDataReceived = new Date().getTime();

    // Read sensor mapping from JSON file.
    Object.keys(deviceMappingJson).forEach(function(key) {
        try {
            pushHASensorData(key, parseDataFromTemplateParams(values[count], key))
        } catch(e) {
            log.error(e);
        }
        count++;
    });

    if(!receivedSerialData) {
        log.info("Received data from sensor, and posted to MQTT... Program is up and running!"); 
        receivedSerialData = true;
    }

});


function parseDataFromTemplateParams(data, configItem) {
    let returnValue;
    const valueType = deviceMappingJson[configItem].type;
    log.debug(`Parsing value ${data} with type ${valueType} for config item ${configItem}`);
    switch(valueType) {

      case "float":

        // Parse value
        returnValue = Number(parseFloat(data).toFixed(fractionDigits));
        log.debug(`Parsed float value ${returnValue} for config item ${configItem} with ${fractionDigits} fraction digits`);

        // Check for NaN value
        if (isNaN(returnValue)) {
            log.warn(`Nan value detected for float type ${configItem} config item. Original value : ${data}. Returning 0.0 instead`);
            returnValue = 0.0;
        }

        // Invert negative value if parameterized
        if (returnValue < 0 && invertNegativeValues) {
            log.debug(`Inverting negative return value ${returnValue} for config item ${configItem}`);
            returnValue = -returnValue;
        }
        // Set value to 0.0 if inferior to threshold
        if (returnValue < sensorValueThreshold) {
            log.debug(`Return value ${returnValue} inferior to threshold ${sensorValueThreshold} for config item ${configItem}. Returning 0.0 instead`);
            returnValue = 0.0;
        }
        
        break;

      case "integer":

        // Parse value
        returnValue = parseInt(data);

        // Check for NaN value
        if (isNaN(returnValue)) {
            log.warn(`Nan value detected for integer type ${configItem} config item. Original value : ${data}. Returning 0 instead`);
            returnValue = 0;
        }

        // Invert negative value if parameterized
        if (returnValue < 0 && invertNegativeValues) {
            log.debug(`Inverting negative return value ${returnValue} for config item ${configItem}`);
            returnValue = -returnValue;
        }
        // Set value to 0 if inferior to threshold
        if (returnValue < sensorValueThreshold) {
            log.debug(`Return value ${returnValue} inferior to threshold ${sensorValueThreshold} for config item ${configItem}. Returning 0 instead`);
            returnValue = 0;
        }

        break;
      case "string":
        returnValue = data;
        break;
      default:
        returnValue = data;
    }

    log.debug(`Parsed value ${returnValue} from raw value ${data} with type ${valueType} for config item ${configItem}`);
    return returnValue;
}

function createHASensor(name, unit_of_measurement, icon) {
    mqttClient.publish(
        `${discoveryPrefix}/sensor/${identifier}_${name}/config`,
        `{
            "name": "${name}",
            "unit_of_measurement": "${unit_of_measurement}",
            "state_topic": "${discoveryPrefix}/sensor/${identifier}_${name}",
            "icon": "mdi:${icon}",
            "unique_id": "${identifier}_${name}",
            "device": {
                "name": "LeChacal Energy Monitor",
                "identifiers": ["LeChacal", "Energy", "Monitor"],
                "manufacturer": "LeChacal",
                "model": "${deviceMapping.replace(".json", "")}",
                "sw_version": "latest",
                "via_device": "docker-lechacal-homeassistant"
            },
            "availability": {
                "topic": "lechacal/${identifier}/availability",
                "payload_available": "online",
                "payload_not_available": "offline"
            }
        }`, 
        {
            retain: true,
        }
    );
}

function pushHASensorData(name, data) {
    mqttClient.publish(
        `${discoveryPrefix}/sensor/${identifier}_${name}`, `${data}`
    );
}

function createHASensors() {
    log.debug(`Creating HA sensors...`);

    // Logic to Auto-create HA device...
    Object.keys(deviceMappingJson).forEach(function(key) {
        createHASensor(key, deviceMappingJson[key].unit_of_measurement, deviceMappingJson[key].icon)
    });
    mqttClient.publish(`lechacal/${identifier}/availability`, "online");
}

function updateAvailability() {
    let currentTime = new Date().getTime();
    let millisSinceLastData = currentTime - lastDataReceived;
    let available;
    if(millisSinceLastData < (1000 * 60 * 5)) {
        available = "online"
    } else {
        available = "offline"
    }
    mqttClient.publish(`lechacal/${identifier}/availability`, available);
}

setTimeout(updateAvailability, 1000 * 60 * 5);
