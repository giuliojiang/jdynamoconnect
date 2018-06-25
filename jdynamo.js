// Utility AWS dynamoDB connection library
// 
// Expects the usage of a table containing atomic counters:
//     key: "kname"
//     countervalue: "kval"
// The atomic counters table is used to generate unique identifiers for other tables
//
// Other tables have as key "kid", an ID generated using the counters table
// All other fields are arbitrary

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

    context.jdynamoconnect = {};
    context.jdynamoconnect.dynamodb = new AWS.DynamoDB();
    context.jdynamoconnect.docclient = new AWS.DynamoDB.DocumentClient();
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
// Inserts a document in the table
// Automatically generates a new unique ID, and sets it to the "kid" field of the document
// <context> jdynamo context object
// <tableName> name of the table where the new document will be inserted
// <document> javascript key-value object document to be inserted
// <counterTable> table name that has all atomic counters
// <counterName> name of the counter in the atomic counters table
// callback(err)
//     <err> any error during processing
pub.insertDocument = function(context, tableName, document, counterTable, counterName, callback) {

    priv.checkContext(context);

    // Get an updated counter
    pub.atomicIncrement(context, counterTable, counterName, function(err, count) {
        if (err) {
            callback(err);
            return;
        }

        var kid = count.toString();
        document.kid = kid;

        var params = {
            TableName: tableName,
            Item: document
        };

        context.jdynamoconnect.docclient.put(params, function(err) {
            if (err) {
                callback(err);
            } else {
                callback();
            }
        });
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