require("dotenv").config();
const http = require("http");
const kafka = require('kafka-node');

const port = process.env.PORT || 3100;

const requestHandler = (request, response) => {
    console.log(request.url)
    response.end('Hello Node.js Server!')
}

const server = http.createServer(requestHandler)

server.listen(port, (err) => {
    if (err) {
        return console.log('something bad happened', err)
    }

    console.log(`server is listening on ${port}`)
})

const kafkaClient = new kafka.KafkaClient({ kafkaHost: process.env.KAFKA_BROKER });
const kafkaConsumer = kafka.Consumer;

const consumer = new kafkaConsumer(kafkaClient, [
    { topic: process.env.KAFKA_TOPIC }
])

consumer.on('message', function (message) {
    console.log(message);
});

consumer.on("error", err => {
    console.error(err);
})