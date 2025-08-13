require('dotenv').config();
const mosca = require('mosca');

// configure the MQTT broker settings
const settings = {
port: process.env.MQTT_BROKER_PORT
};

// create a new MQTT broker instance
const broker = new mosca.Server(settings);

// authenticate clients using environment variables
broker.authenticate = async function(client, username, password, callback) {
  const passwordString = password ? password.toString('utf8') : ''; // Handle undefined password
  const authorized = username === (process.env.MQTT_USERNAME) && 
                    passwordString === (process.env.MQTT_PASSWORD);
  if (authorized) {
    client.user = username; // store the username on the client object for later use
  }
  callback(null, authorized);
};

// log client connections and disconnections
broker.on('clientConnected', function(client) {
console.log(`Client connected: ${client.id}, username: ${client.user}`);
});

broker.on('clientDisconnected', function(client) {
console.log(`Client disconnected: ${client.id}, username: ${client.user}`);
});

broker.on('published', (packet)=>{
message = packet.payload.toString()
console.log(message)

})

console.log('MQTT broker is running');
