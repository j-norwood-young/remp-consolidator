require("dotenv").config();
const http = require("http");
const kafka = require('kafka-node');
const elasticsearch = require("elasticsearch");
const indexes = require("./indexes.json");
const redis = require("redis").createClient();

const redis_key = process.env.REDIS_KEY || "remp-consolidator";

redis.on("error", err => {
    console.error(err);
});

const port = process.env.PORT || 3100;

const requestHandler = (req, res) => {
    if(req.method == 'POST') {
        let jsonString = '';
        req.on('data', function (data) {
            jsonString += data;
        });
        req.on('end', function () {
            try {
                let data = JSON.parse(jsonString);
                queueData(data);
                // console.log(data);
            } catch(err) {
                
            }
        });
    } else {
        res.end('This is the remp-consolidator')    
    }
    // console.log(request.url)
}

const server = http.createServer(requestHandler)

server.listen(port, (err) => {
    if (err) {
        return console.error('something bad happened', err)
    }

    console.log(`server is listening on ${port}`)
})

const esclient = new elasticsearch.Client({
    host: 'localhost:9200',
    // log: 'trace'
});

const kafkaOptions = {
	kafkaHost: '127.0.0.1:9092',
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
var ids = {};

const redisAdd = (key, val) => {
    return new Promise((resolve, reject) => {
        redis.SADD(key, val, (err, result) => {
            if (err) return reject(err);
            return resolve(result);
        });
    });
}

const redisIsMember = (key, val) => {
    return new Promise((resolve, reject) => {
        redis.SISMEMBER(key, val, (err, result) => {
            if (err) return reject(err);
            return resolve(result);
        });
    });
}

const checkCache = async () => {
    try {
        if (cache.length >= process.env.CACHE_SIZE) {
            await esclient.bulk({ maxRetries: 5, body: cache });
            cache = [];
            console.log(`Flushed cache, itteration ${ count++ }`);
        }
    } catch(err) {
        console.error(err);
    }
}

consumer.on('message', async (message) => {
    try {
        let [ d0, index, d1, json, d2, timestamp ] = message.value.split(/(^\S*)(\s_json=")(.*\})(\"\s)(\d.*$)/);
        json = JSON.parse(json.replace(/\\/g,""));
        const config = indexes.find(config => config.namepass === index);
        if (config) {
            const key = `${redis_key}-${index}`;
            if (!(await redisIsMember(key, json[config.id_field]))) {
                json.time = new Date(timestamp);
                json._timestamp = new Date();
                await redisAdd(key, json[config.id_field]);
                cache.push({
                    index: {
                        _index: config.index_name,
                        _type: "_doc",
                    }
                }, json);
            }
        }
        await checkCache();
        // esclient.bulk()
    } catch(err) {
        console.error(err);
    }
});

consumer.on("error", err => {
    console.error(err);
})