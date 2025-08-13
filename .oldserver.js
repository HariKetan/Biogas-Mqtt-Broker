const express = require('express');
const mqtt = require('mqtt');
const { Pool } = require('pg');
const dotenv = require('dotenv');

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

const pool = new Pool({
  user: process.env.DB_USER || 'biogas',
  host: process.env.DB_HOST || 'localhost',
  database: process.env.DB_NAME || 'biogas',
  password: process.env.DB_PASSWORD || 'biogas',
  port: process.env.DB_PORT || 5432,
});

const clientId = 'c12';
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL, {
  clientId,
  username: process.env.MQTT_USERNAME || 'biogas',
  password: process.env.MQTT_PASSWORD || 'biogas',
  port: process.env.MQTT_BROKER_PORT || 1883,
});

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  mqttClient.subscribe('/bio', (err) => {
    if (!err) {
      console.log(`Subscribed to topic: /bio`);
    } else {
      console.error(`Error subscribing to topic: ${err}`);
    }
  });
});

mqttClient.on('message', async (receivedTopic, message) => {
  try {
    const messageObj = JSON.parse(message);
    console.log('Received MQTT message:', messageObj);

    const topicParts = receivedTopic.split('/'); // Split the topic into parts
    if (topicParts.length < 3) {
      console.error('Invalid topic format:', receivedTopic);
      return;
    }

    const apartmentId = topicParts[2]; // The third part of the topic is the apartment ID
    const sensorType = topicParts[3]; // The fourth part of the topic is the sensor type

    if (!apartmentId || !sensorType) {
      console.error('Invalid topic format:', receivedTopic);
      return;
    }

    const deviceId = messageObj.mac_id;
    const description = messageObj.description;

    // Check if the apartment ID exists in the database, and insert it if not
    const checkApartmentQuery = 'SELECT ap_id FROM apartment WHERE ap_id = $1';
    const checkApartmentValues = [apartmentId];
    const apartmentResult = await pool.query(checkApartmentQuery, checkApartmentValues);

    if (apartmentResult.rows.length === 0) {
      const insertApartmentQuery = 'INSERT INTO apartment (ap_id) VALUES ($1)';
      const insertApartmentValues = [apartmentId];
      await pool.query(insertApartmentQuery, insertApartmentValues);
    }

    const deviceQuery = 'INSERT INTO device (device_id, description, appt_id) VALUES ($1, $2, $3) ON CONFLICT (device_id) DO NOTHING';
    const deviceInsertValues = [deviceId, description, apartmentId];
    await pool.query(deviceQuery, deviceInsertValues);

    const sensorIdQuery = 'SELECT s_id FROM sensor WHERE sensor_type = $1';
    const sensorIdValues = [sensorType];
    const sensorResult = await pool.query(sensorIdQuery, sensorIdValues);

    if (sensorResult.rows.length === 0) {
      console.error('Invalid sensor type:', sensorType);
      return;
    }

    const sensorId = sensorResult.rows[0].s_id;

    const insertQuery = `
      INSERT INTO sensor_value (s_id, device_id, value, u_time)
      VALUES ($1, $2, $3, NOW())
    `;

    const insertValues = [sensorId, deviceId, messageObj.value];
    const result = await pool.query(insertQuery, insertValues);

    console.log('Inserted into the database:', result.rows[0]);
  } catch (err) {
    console.error('Error processing MQTT message:', err);
  }
});

mqttClient.on('error', (error) => {
  console.error(`MQTT Error: ${error}`);
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
