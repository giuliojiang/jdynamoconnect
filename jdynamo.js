var AWS = require("aws-sdk");

var pub = {};
var priv = {};

AWS.config.update({
    region: "eu-west-2"
});

var dynamodb = new AWS.DynamoDB();

// <tableName> the name of the DynamoDB table
// <counterName> the name of the counter in the table, column "name"
// callback(err, count)
pub.atomicIncrement = function(tableName, counterName, callback) {

    var params = {
        UpdateExpression: "SET kval = kval + :incr",
        TableName: tableName,
        ReturnValues: "UPDATED_NEW",
        Key: {
            "kname": {
                S: counterName
            }
        },
        ExpressionAttributeValues: {
            ":incr": {
                N: "1"
            }
        }
    };

    dynamodb.updateItem(params, function(err, data) {
        if (err) {
            callback(err);
        } else {
            try {
                var attributes = data.Attributes;
                var kval = attributes.kval;
                var count = kval.N;
                callback(null, count);
            } catch (err) {
                callback(err);
            }
        }
    });

};









module.exports = pub;