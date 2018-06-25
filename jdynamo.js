var AWS = require("aws-sdk");

var pub = {};
var priv = {};

// ============================================================================
// Initializes the dynamodb database object
// The context object is populated/overwritten at the field key "jdynamoconnect"
// The jdynamoconnect context section is:
// {
//     dynamodb: AWS DynamoDB object
// }
// <context> any object, or an empty object {}
// <region>  an AWS region string, such as "eu-west-2"
pub.init = function(context, region) {
    if (context.jdynamoconnect) {
        throw new Error("The field jdynamoconnect is already used in the context object. You can call init only once");
    }

    AWS.config.update({
        region: region
    });

    var dynamodb = new AWS.DynamoDB();
    context.jdynamoconnect = {};
    context.jdynamoconnect.dynamodb = dynamodb;
}

// ============================================================================
// Atomically increases an item
// <tableName> the name of the DynamoDB table
// <counterName> the name of the counter in the table, column "name"
// callback(err, count)
//     <err>   any error in the operation
//     <count> is the updated count number
pub.atomicIncrement = function(context, tableName, counterName, callback) {

    priv.checkContext(context);

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

    context.jdynamoconnect.dynamodb.updateItem(params, function(err, data) {
        if (err) {
            callback(err);
        } else {
            try {
                var attributes = data.Attributes;
                var kval = attributes.kval;
                var count = kval.N;
                var count = parseInt(count);
                callback(null, count);
            } catch (err) {
                callback(err);
            }
        }
    });

};

// ============================================================================
// Checks whether the context has been initialized for jdynamoconnect
priv.checkContext = function(context) {
    if (!context.jdynamoconnect) {
        throw new Error("context object not initialized. Please call jdynamoconnect.init");
    }
}







module.exports = pub;