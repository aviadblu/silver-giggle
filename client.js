const express = require('express');
const bodyParser = require('body-parser');
const amqp = require('amqplib');
const SERVER_PORT = 7777;
const QUEUE_NAME = 'demo-queue';
const CONNECTION_STRING = 'amqp://admin:admin@localhost:5672/';

const RabbitMqClientService = () => {
    let connection, channel;
    const init = async () => {
        connection = await amqp.connect(CONNECTION_STRING);
        channel = await connection.createChannel();
        await channel.assertQueue(QUEUE_NAME, {
            durable: false
        });
    };

    const send = async (msg) => {
        return await channel.sendToQueue(QUEUE_NAME, Buffer.from(msg));
    }

    return Object.freeze({
        init,
        send
    })
};

const Server = (RabbitMQ) => {
    const app = express();
    app.use(bodyParser.json()); // support json encoded bodies
    app.use(bodyParser.urlencoded({extended: true})); // support encoded

    app.post('/message', async (req, res) => {
        if (req.body && req.body.data) {
            try {
                await RabbitMQ.send(req.body.data);
                res.send({
                    status: 'OK'
                });
            } catch (e) {
                res.status(500).send({status: 'error', details: e});
            }
        } else {
            res.status(400).send({status: 'Bad request'});
        }
    });

    app.listen(SERVER_PORT);
    console.log(`Server listen on port ${SERVER_PORT}, POST /message with {data: "my message"}`);
};


const Main = async () => {
    const RabbitMQ = RabbitMqClientService();
    await RabbitMQ.init();
    Server(RabbitMQ);
};
Main();