const express = require("express");
const mqtt = require("mqtt");
const { Pool } = require("pg");
const dotenv = require("dotenv");

dotenv.config({ path: '.env' });

const app = express();
const port = process.env.PORT || 3004;

const pool = new Pool({
	user: process.env.DB_USER || "biogas",
	host: process.env.DB_HOST || "localhost",
	database: process.env.DB_NAME || "biogas",
	password: process.env.DB_PASSWORD || "biogas",
	port: process.env.DB_PORT || 5432,
});

const clientId = "c12";
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL, {
	clientId,
	username: process.env.MQTT_USERNAME || "biogas",
	password: process.env.MQTT_PASSWORD || "biogas",
	port: process.env.MQTT_BROKER_PORT || 1883,
});

// Enhanced sensor transformation function
function applySensorTransformations(sensorId, regAdd, value, sensorName) {
	let transformedValue = value;

	// Define transformations based on sensor type and register address
	const transformations = {
		// pH sensor (existing)
		"2_2": (val) => val / 100,

		// Temperature sensor (existing)
		"2_3": (val) => val, // No transformation needed

		// Voltage sensors (existing)
		"3_0": (val) => val, // R phase voltage
		"3_2": (val) => val, // Y phase voltage
		"3_4": (val) => val, // B phase voltage

		// Frequency sensor (existing)
		"3_56": (val) => val,

		// Weight sensor (existing)
		"7_0": (val) => val,

		// New moisture and humidity sensors
		"8_0": (val) => val / 10, // Moisture sensor with scaling
		"8_1": (val) => val / 10, // Humidity sensor with scaling

		// New sensors for device 1368
		"1_0": (val) => val, // Sensor1 from device 1368
		"2_0": (val) => val, // Sensor2 from device 1368

		// Multiple methane sensors for device 1368
		"1_0_1": (val) => val, // Methane1
		"1_0_2": (val) => val, // Methane2
		"1_0_3": (val) => val, // Methane3
		"1_0_4": (val) => val, // Methane4
		"1_0_5": (val) => val, // Methane5
		"1_0_6": (val) => val, // Methane6

		// Multiple pH sensors for device 1368
		"2_0_1": (val) => val, // pH1
		"2_0_2": (val) => val, // pH2
		"2_0_3": (val) => val, // pH3
		"2_0_4": (val) => val, // pH4
	};

	const transformationKey = `${sensorId}_${regAdd}`;
	const transformation = transformations[transformationKey];

	if (transformation) {
		transformedValue = transformation(value);
		console.log(
			`Applied transformation for ${sensorName}: ${value} → ${transformedValue}`
		);
	} else {
		console.log(
			`No transformation defined for sensor ${sensorName} (${sensorId}_${regAdd})`
		);
	}

	return transformedValue;
}

mqttClient.on("connect", () => {
	console.log("Connected to MQTT broker");

	mqttClient.subscribe("#", (err) => {
		//Subscribes all the topics "#"
		if (!err) {
			console.log(`Subscribed to topic: #`);
		} else {
			console.error(`Error subscribing to topic: ${err}`);
		}
	});
});

mqttClient.on("message", async (receivedTopic, message) => {
	try {
		const messageObj = JSON.parse(message);
		console.log("Received message:", messageObj);

		// Check for new format (simulation script format)
		if (
			messageObj.device_id &&
			messageObj.slave_id &&
			messageObj.reg_add !== undefined &&
			messageObj.value !== undefined
		) {
			// New format from simulation script
			const device_id = messageObj.device_id;
			const slave_id = messageObj.slave_id;
			const reg_add = messageObj.reg_add;
			const value = messageObj.value;
			const d_ttime = messageObj.d_ttime;

			console.log(
				`Processing new format: device_id=${device_id}, slave_id=${slave_id}, reg_add=${reg_add}, value=${value}`
			);

			// Insert device if not exists
			const deviceQuery =
				"INSERT INTO DEVICE (DEVICE_ID) VALUES ($1) ON CONFLICT (DEVICE_ID) DO NOTHING";
			const deviceInsertValues = [device_id];
			await pool.query(deviceQuery, deviceInsertValues);

			// Check if sensor parameter exists
			const sensorParameterQuery =
				"SELECT SLAVE_ID, REG_ADD, KEYS, SIUNIT FROM SENSOR_PARAMETERS WHERE DEVICE_ID = $1 AND SLAVE_ID = $2 AND REG_ADD = $3";
			const sensorParameterValues = [device_id, slave_id, reg_add];
			const sensorParameterResult = await pool.query(
				sensorParameterQuery,
				sensorParameterValues
			);

			if (sensorParameterResult.rows.length === 0) {
				console.error(
					"No sensor parameters found for device ID:",
					device_id,
					"slave_id:",
					slave_id,
					"reg_add:",
					reg_add
				);
				return;
			}

			const sensorParam = sensorParameterResult.rows[0];
			const sensorKey = sensorParam.keys;
			const sensorUnit = sensorParam.siunit;

			console.log(
				`Processing sensor: ${sensorKey} (${sensorUnit}) - Raw Value: ${value}`
			);

			// Apply sensor transformations
			let transformedValue = applySensorTransformations(
				slave_id,
				reg_add,
				value,
				sensorKey
			);

			// Insert sensor value with correct column name
			const insertQuery = `
				INSERT INTO SENSOR_VALUE (DEVICE_ID, SLAVE_ID, REG_ADD, VALUE, U_TIME, D_TTIME)
				VALUES ($1, $2, $3, $4, NOW(), $5)
			`;

			const insertValues = [
				device_id,
				slave_id,
				reg_add,
				transformedValue,
				d_ttime,
			];
			const result = await pool.query(insertQuery, insertValues);

			console.log(
				`Successfully inserted ${sensorKey} value: ${transformedValue} ${sensorUnit}`
			);
		} else if (
			messageObj.ID &&
			messageObj.SL_ID &&
			messageObj.RegAd !== undefined &&
			messageObj.D1 !== undefined
		) {
			// Old format (legacy support)
			console.log("Processing legacy format message");

			const deviceQuery =
				"INSERT INTO DEVICE (DEVICE_ID) VALUES ($1) ON CONFLICT (DEVICE_ID) DO NOTHING";
			const deviceInsertValues = [messageObj.ID];
			await pool.query(deviceQuery, deviceInsertValues);

			const sensorParameterQuery =
				"SELECT SLAVE_ID, REG_ADD, KEYS, SIUNIT FROM SENSOR_PARAMETERS WHERE DEVICE_ID = $1";
			const sensorParameterValues = [messageObj.ID];
			const sensorParameterResult = await pool.query(
				sensorParameterQuery,
				sensorParameterValues
			);

			const result2 = sensorParameterResult.rows.map((row) => ({
				slave_id: row.slave_id,
				reg_add: row.reg_add,
				keys: row.keys,
				siunit: row.siunit,
			}));
			console.log("Available sensor parameters:", result2);

			if (sensorParameterResult.rows.length === 0) {
				console.error(
					"No sensor parameters found for device ID:",
					messageObj.ID
				);
				return;
			}

			const slaveId = messageObj.SL_ID;
			const regAdd = messageObj.RegAd;
			const device_id = messageObj.ID;
			const d_time = messageObj.DATE + " " + messageObj.TIME;

			const sensorParameters = result2.find(
				(param) => param.slave_id === slaveId && param.reg_add === regAdd
			);

			if (!sensorParameters) {
				console.error(
					"No matching sensor parameters found for device ID, SL_ID, and RegAd:",
					messageObj.ID,
					slaveId,
					regAdd
				);
				return;
			}

			const sensorId = sensorParameters.slave_id;
			const sensorKey = sensorParameters.keys;
			const sensorUnit = sensorParameters.siunit;

			// Handle multiple sensor values for device 1368
			if (device_id === '1368') {
				const sensorValues = [];
				
				// Collect all available D values
				for (let i = 1; i <= 6; i++) {
					const dValue = messageObj[`D${i}`];
					if (dValue !== undefined && dValue !== null) {
						sensorValues.push({
							value: dValue,
							index: i
						});
					}
				}

				console.log(`Processing ${sensorValues.length} values for ${sensorKey} (${sensorUnit})`);

				// Insert each sensor value with a unique identifier
				for (const sensorData of sensorValues) {
					let insertValue = sensorData.value;
					
					// Apply sensor transformations using the enhanced function
					insertValue = applySensorTransformations(
						sensorId,
						regAdd,
						insertValue,
						sensorKey
					);

					// Create a unique register address for each sensor value
					const uniqueRegAdd = `${regAdd}_${sensorData.index}`;

					const insertQuery = `
						INSERT INTO SENSOR_VALUE (DEVICE_ID, SLAVE_ID, REG_ADD, VALUE, U_TIME, D_TTIME)
						VALUES ($1, $2, $3, $4, NOW(), $5)
					`;

					const insertValues = [device_id, sensorId, uniqueRegAdd, insertValue, d_time];
					const result = await pool.query(insertQuery, insertValues);

					console.log(
						`Successfully inserted ${sensorKey} value ${sensorData.index}: ${insertValue} ${sensorUnit}`
					);
				}
			} else {
				// Original single value processing for other devices
				let insertValue = messageObj.D1;

				console.log(
					`Processing sensor: ${sensorKey} (${sensorUnit}) - Raw Value: ${insertValue}`
				);

				// Apply sensor transformations using the enhanced function
				insertValue = applySensorTransformations(
					sensorId,
					regAdd,
					insertValue,
					sensorKey
				);

				const insertQuery = `
					INSERT INTO SENSOR_VALUE (DEVICE_ID, SLAVE_ID, REG_ADD, VALUE, U_TIME, D_TTIME)
					VALUES ($1, $2, $3, $4, NOW(), $5)
				`;

				const insertValues = [device_id, sensorId, regAdd, insertValue, d_time];
				const result = await pool.query(insertQuery, insertValues);

				console.log(
					`Successfully inserted ${sensorKey} value: ${insertValue} ${sensorUnit}`
				);
			}
		} else {
			console.error("Invalid message format:", messageObj);
		}
	} catch (err) {
		console.error("Error processing MQTT message:", err);
	}
});

mqttClient.on("error", (error) => {
	console.error(`MQTT Error: ${error}`);
});

app.listen(port, () => {
	console.log(`Server is running on port ${port}`);
});
