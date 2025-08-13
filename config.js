require('dotenv').config();

module.exports = {
	brokerUrl: process.env.MQTT_BROKER_URL,
	brokerPort: process.env.MQTT_BROKER_PORT,
	username: process.env.MQTT_USERNAME,
	password: process.env.MQTT_PASSWORD,
	topic: process.env.MQTT_TOPIC,
};
