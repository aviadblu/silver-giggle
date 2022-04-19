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

    const getConnection = () => connection;

    const send = async (msg) => {
        return await channel.sendToQueue(QUEUE_NAME, Buffer.from(msg));
    }

    return Object.freeze({
        init,
        getConnection,
        send
    })
};

const Main = async () => {
    const RabbitMQ = RabbitMqClientService();
    await RabbitMQ.init();


    const args = process.argv.slice(2);
    const noOfMsgs = parseInt(args[0]);
    if (noOfMsgs > 0) {
        for (let i = 0; i < noOfMsgs; i++) {
            const msg = `Hello ${i}`;
            await RabbitMQ.send(msg);
        }
    }
    setTimeout(() => {
        RabbitMQ.getConnection().close();
        process.exit(0);
    }, 1000);
};
Main();