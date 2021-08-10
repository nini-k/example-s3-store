const {
          S3Client,
          PutObjectCommand,
          UploadPartCommand,
          DeleteObjectCommand,
          AbortMultipartUploadCommand,
          CreateMultipartUploadCommand,
          CompleteMultipartUploadCommand,
          ListMultipartUploadsCommand
      } = require("@aws-sdk/client-s3");

class S3Store {
    constructor(opts) {
        this._s3c = new S3Client(opts);
    }

    /**
     * @param {string} bucket
     * @param {string} path
     * @param {Readable | ReadableStream | ReadStream} file
     * @param {number} chunkSize
     * @return {Promise<*>}
     */
    async uploadFileByParts(bucket, path, file, chunkSize = 25 * 1024 * 1024) {
        const createCmd = new CreateMultipartUploadCommand({
            Bucket: bucket,
            Key: path,
            ContentType: "application/text",
            ContentEncoding: "gzip",
            ACL: "private"
        });
        const {UploadId} = await this._s3c.send(createCmd);
        if (!UploadId) {
            throw Error("create multipart upload returned empty UploadId");
        }
        try {
            let buffers = [], totalBuffersLength = 0;
            let PartNumber = 1, Parts = [];
            for await (const chunk of file) {
                if (totalBuffersLength < chunkSize) {
                    buffers.push(chunk);
                    totalBuffersLength += chunk.length;
                    continue;
                }

                const body = Buffer.concat(buffers);
                const ETag = await this.__uploadPart(bucket, path, PartNumber, UploadId, body);
                buffers = [], totalBuffersLength = 0;
                Parts.push({ETag, PartNumber});
                PartNumber++;
            }
            if (buffers.length) {
                const body = Buffer.concat(buffers);
                const ETag = await this.__uploadPart(bucket, path, PartNumber, UploadId, body);
                Parts.push({ETag, PartNumber});
            }
            console.log(Parts);
            const completeCmd = new CompleteMultipartUploadCommand({
                Bucket: bucket,
                Key: path,
                UploadId,
                MultipartUpload: {Parts}
            });
            return this._s3c.send(completeCmd);
        } catch (err) {
            console.error(err);
            const abortCmd = new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: path,
                UploadId
            });
            await this._s3c.send(abortCmd);
        }
        return null;
    }

    /**
     * @param {string} Bucket
     * @param {string} Key
     * @param {number} PartNumber
     * @param {string} UploadId
     * @param {Buffer} Body
     * @return {Promise<string>}
     * @private
     */
    async __uploadPart(Bucket, Key, PartNumber, UploadId, Body) {
        const uploadCmd = new UploadPartCommand({Bucket, Key, PartNumber, UploadId, Body});
        const {ETag} = await this._s3c.send(uploadCmd);
        if (!ETag) {
            const abortCmd = new AbortMultipartUploadCommand({Bucket, Key, UploadId});
            await this._s3c.send(abortCmd);
            throw Error("upload part returned empty ETag");
        }

        return ETag;
    }
}

module.exports = S3Store;
