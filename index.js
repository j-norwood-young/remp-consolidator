require("dotenv").config();
const kafka = require('kafka-node');
const elasticsearch = require("elasticsearch");
const configs = require("./configs.json");

const esclient = new elasticsearch.Client({
    host: process.env.ES_SERVER
});

const kafkaOptions = {
	kafkaHost: process.env.KAFKA_SERVER,
	groupId: process.env.KAFKA_GROUP,
	autoCommit: true,
	autoCommitIntervalMs: 5000,
	sessionTimeout: 15000,
 	fetchMaxBytes: 10 * 1024 * 1024, // 10 MB
	protocol: ['roundrobin'],
	fromOffset: 'earliest',
	outOfRangeOffset: 'earliest'
}

const kafkaConsumerGroup = kafka.ConsumerGroup;
const consumer = new kafkaConsumerGroup(kafkaOptions, process.env.KAFKA_TOPIC)

var cache = [];
var count = 0;

const esBulk = (params) => {
    return new Promise((resolve, reject) => {
        esclient.bulk(params, (err, result) => {
            if (err) return reject(err);
            return resolve(result);
        });
    })
}

const cache_size = process.env.CACHE_SIZE || 1000;

const flush = async () => {
    if (cache.length > cache_size) {
        try {
            consumer.pause();
            if (process.env.DEBUG) {
                console.log("Cache length:", cache.length);
            }
            const result = await esBulk({ maxRetries: 5, body: cache });
            cache = [];
            if (process.env.DEBUG) {
                console.log(`Flushed cache, loop ${count++}, items ${result.items.length}`);
                for (let item of result.items) {
                    if (item.index.error) {
                        console.error(item.index.error);
                    }
                }
            }
            consumer.resume();    
        } catch(err) {
            consumer.resume();
            console.error(err);
        }
    }
}

consumer.on('message', async (message) => {
    try {
        json = JSON.parse(message.value);
        // console.log({ json });
        for(config of configs) {
            try {
                if (config.namepass === json.index) {
                    cache.push({
                        index: {
                            _index: config.index_name,
                            _type: "_doc",
                        }
                    }, json);
                    // console.log("Cached", json.index);
                }
            } catch(err) {
                console.error(err);
            }
        };
    } catch(err) {
        console.error(err);
    }
});

consumer.on("error", err => {
    console.error(err);
})

const interval = process.env.TEST_INTERVAL || 5000;

setInterval(flush, interval);