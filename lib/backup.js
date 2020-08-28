const dotenv = require("dotenv");
const AWS = require("aws-sdk");

dotenv.config();

const createPath = () => {
  const dt = new Date();
  return [
    dt.getUTCFullYear(),
    ...[
      dt.getUTCMonth() + 1,
      dt.getUTCDate(),
      dt.getUTCHours(),
      dt.getUTCMinutes(),
      dt.getUTCSeconds(),
    ].map((i) => i.toString().padStart(2, "0")),
  ].join("/");
};

module.exports.backupToS3 = function (name, body) {
  const s3 = new AWS.S3();
  s3.putObject({
    Bucket: process.env.S3_BUCKET,
    Key: `${createPath()}/${name}`,
    Body: body,
  }).promise();
};
