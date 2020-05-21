const config = require("config");
const kafka = require('kafka-node');
const elasticsearch = require("elasticsearch");
const configs = require("./configs.json");

const esclient = new elasticsearch.Client({
    host: config.es_server
});

const kafkaOptions = {
	kafkaHost: config.kafka_server,
	groupId: config.kafka_group,
	autoCommit: true,
	autoCommitIntervalMs: 5000,
	sessionTimeout: 15000,
 	fetchMaxBytes: 10 * 1024 * 1024, // 10 MB
	protocol: ['roundrobin'],
	fromOffset: 'earliest',
	outOfRangeOffset: 'earliest'
}

const kafkaConsumerGroup = kafka.ConsumerGroup;
const consumer = new kafkaConsumerGroup(kafkaOptions, config.kafka_topic)

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

const cache_size = config.cache_size || 1000;

const flush = async () => {
    if (cache.length > cache_size) {
        try {
            consumer.pause();
            if (config.debug) {
                console.log("Cache length:", cache.length);
            }
            const result = await esBulk({ maxRetries: 5, body: cache });
            cache = [];
            if (config.debug) {
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
        if (config.debug) {
            console.log({ json });
        }
        for(let message_config of configs) {
            try {
                if (message_config.namepass === json.index) {
                    cache.push({
                        index: {
                            _index: message_config.index_name,
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

const interval = config.test_interval || 5000;
console.log(`Listening for kafka messages; flushing cache every ${interval / 1000}s`);
setInterval(flush, interval);