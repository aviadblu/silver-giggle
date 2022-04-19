const amqp = require('amqplib');
const QUEUE_NAME = 'demo-queue';
const CONNECTION_STRING = 'amqp://admin:admin@localhost:5672/';

const RabbitMqWorkerService = () => {
    let connection, channel;
    const init = async () => {
        connection = await amqp.connect(CONNECTION_STRING);
        channel = await connection.createChannel();
        await channel.assertQueue(QUEUE_NAME, {
            durable: false
        });
    };

    const consumer = (cb) => {
        return channel.consume(QUEUE_NAME, cb);
    };

    const getChannel = () => {
        return channel;
    }

    return Object.freeze({
        init,
        getChannel,
        consumer
    })
};

const sleep = async (time) => {
    return new Promise((resolve) => {
        setTimeout(resolve, time);
    });
};

const doSomeAsyncBla = async (msg) => {
    await sleep(1000);
    return `doSomeAsyncBla for msg ${msg} completed!`;
};

const Main = async () => {
    const RabbitMQ = RabbitMqWorkerService();
    await RabbitMQ.init();
    console.log(`Worker ready to handle message from queue: '${QUEUE_NAME}'`);
    RabbitMQ.consumer(async (rawMsg) => {
        const ch = RabbitMQ.getChannel();
        const msg = rawMsg.content.toString();
        const result = await doSomeAsyncBla(msg);
        console.log(result);
        ch.ack(rawMsg)
    });
};
Main();
