import { v4 } from 'uuid';
import {
  SendMessageRequest,
  ReceiveMessageCommandInput,
  Message,
  MessageAttributeValue,
  SQS,
  ReceiveMessageResult,
} from '@aws-sdk/client-sqs';
import { S3 } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import { SQSEvent } from 'aws-lambda';

export type callbackFnType = (...args: any[]) => void;
export type preRequestFuncType = () => any;
export type postRequestFuncType = (...args: any[]) => any;
type SendTransform = (messageSend: SendMessageRequest) => {
  s3Content: string | null | undefined;
  messageBody: string | null | undefined;
};
type ReceiveTransform = (mesasgeRecv: Message, s3Content?: string) => string | undefined;

type LogMethod = (message: string, metadata?: any) => void;
export interface ILogger {
  debug: LogMethod;
  info: LogMethod;
  warn: LogMethod;
  error: LogMethod;
  child: (metadata: any) => ILogger;
}

const DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144;
const S3_MESSAGE_KEY_MARKER = '-..s3Key..-';
const S3_BUCKET_NAME_MARKER = '-..s3BucketName..-';
const S3_MESSAGE_BODY_KEY = 'S3MessageBodyKey';

export class SQSExtended {
  sqs: SQS;
  s3: S3;
  bucketName: string;
  sendTransform: SendTransform;
  receiveTransform: ReceiveTransform;
  logger: ILogger | undefined;
  static RESERVED_ATTRIBUTE_NAME: string = S3_MESSAGE_BODY_KEY;

  constructor(
    sqs: SQS,
    s3: S3,
    options: {
      bucketName: string;
      alwaysUseS3?: boolean;
      messageSizeThreshold?: number;
      sendTransform?: SendTransform;
      receiveTransform?: ReceiveTransform;
      logger?: ILogger;
    },
  ) {
    this.sqs = sqs;
    this.s3 = s3;
    this.bucketName = options.bucketName;
    this.logger = options.logger?.child({
      from: 'SQSExtended',
    });

    this.sendTransform =
      options.sendTransform || defaultSendTransform(options.alwaysUseS3, options.messageSizeThreshold);
    this.receiveTransform = options.receiveTransform || defaultReceiveTransform();
  }

  private _storeS3Content(key: string, s3Content: string) {
    const params = {
      Bucket: this.bucketName,
      Key: key,
      Body: s3Content,
    };

    return this.s3.putObject(params);
  }

  private async _streamToString(stream: Readable): Promise<string> {
    return new Promise((resolve, reject) => {
      const chunks: any[] = [];
      stream.on('data', (chunk) => {
        chunks.push(Buffer.from(chunk));
      });
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
  }

  private async _getS3Content(bucketName: string, key: string) {
    const params = {
      Bucket: bucketName,
      Key: key,
    };

    this.logger?.info(`Retrieving Extended Message from S3 for key ${key} and bucket ${bucketName}`, params);
    const object = await this.s3.getObject(params);
    return this._streamToString(object.Body);
  }

  private _deleteS3Content(bucketName: string | null, key: string) {
    if (!bucketName) return null;

    const params = {
      Bucket: bucketName,
      Key: key,
    };

    this.logger?.info(`Deleting Extended Message from S3 for key ${key} and bucket ${bucketName}`, params);
    return this.s3.deleteObject(params);
  }

  private _prepareSend(params: any) {
    const sendParams = { ...params };

    const sendObj = this.sendTransform(sendParams);
    const existingS3MessageKey =
      params.MessageAttributes && params.MessageAttributes[SQSExtended.RESERVED_ATTRIBUTE_NAME];
    let s3MessageKey;

    if (!sendObj.s3Content || existingS3MessageKey) {
      sendParams.MessageBody = sendObj.messageBody || existingS3MessageKey.StringValue;
    } else {
      s3MessageKey = v4();
      sendParams.MessageAttributes = addS3MessageKeyAttribute(
        `(${this.bucketName})${s3MessageKey}`,
        sendParams.MessageAttributes,
      );
      sendParams.MessageBody = sendObj.messageBody || s3MessageKey;
    }

    return {
      s3MessageKey,
      sendParams,
      s3Content: sendObj.s3Content,
    };
  }

  sendMessage(params: any, callback?: callbackFnType) {
    if (!this.bucketName) {
      throw new Error('bucketName option is required for sending messages');
    }

    const { s3MessageKey, sendParams, s3Content } = this._prepareSend(params);

    if (!s3MessageKey) {
      return this.sqs.sendMessage(sendParams);
    }

    const request = this.sqs.sendMessage(sendParams);

    return wrapRequest(
      request,
      callback,
      invokeFnBeforeRequest(request, () => this._storeS3Content(s3MessageKey, s3Content!)),
    ).promise();
  }

  private _processReceive() {
    return (response: ReceiveMessageResult) =>
      Promise.all(
        (response.Messages || []).map(async (message) => {
          const { bucketName, s3MessageKey } = getS3MessageKeyAndBucket(
            message.MessageAttributes?.[S3_MESSAGE_BODY_KEY]?.StringValue,
          );

          if (s3MessageKey) {
            /* eslint-disable-next-line no-param-reassign */
            message.Body = this.receiveTransform(message, await this._getS3Content(bucketName, s3MessageKey));
            /* eslint-disable-next-line no-param-reassign */
            message.ReceiptHandle = embedS3MarkersInReceiptHandle(bucketName, s3MessageKey, message.ReceiptHandle);
          } else {
            /* eslint-disable-next-line no-param-reassign */
            message.Body = this.receiveTransform(message);
          }
        }),
      );
  }

  receiveMessage(params: ReceiveMessageCommandInput, callback?: callbackFnType) {
    const modifiedParams = {
      ...params,
      MessageAttributeNames: [...(params.MessageAttributeNames || []), SQSExtended.RESERVED_ATTRIBUTE_NAME],
    };

    const request = this.sqs.receiveMessage(modifiedParams);
    return wrapRequest(request, callback, invokeFnAfterRequest(request, this._processReceive())).promise();
  }

  private _prepareDelete(params: any) {
    return {
      bucketName: extractBucketNameFromReceiptHandle(params.ReceiptHandle),
      s3MessageKey: extractS3MessageKeyFromReceiptHandle(params.ReceiptHandle),
      deleteParams: {
        ...params,
        ReceiptHandle: getOriginReceiptHandle(params.ReceiptHandle),
      },
    };
  }

  deleteMessage(params: any, callback?: callbackFnType) {
    const { bucketName, s3MessageKey, deleteParams } = this._prepareDelete(params);

    if (!s3MessageKey) {
      return this.sqs.deleteMessage(deleteParams);
    }

    const request = this.sqs.deleteMessage(deleteParams);

    return wrapRequest(
      request,
      callback,
      invokeFnBeforeRequest(request, () => this._deleteS3Content(bucketName, s3MessageKey)),
    );
  }

  checkEvent(event: SQSEvent) {
    return Promise.all(
      event.Records.map(async (record) => {
        const { bucketName, s3MessageKey } = getS3MessageKeyAndBucket(
          record.messageAttributes?.[S3_MESSAGE_BODY_KEY]?.stringValue,
        );
        if (s3MessageKey) {
          const s3Content = await this._getS3Content(bucketName, s3MessageKey);
          record.body = s3Content || record.body;
        }
      }),
    );
  }

  eventSucceeded(event: SQSEvent) {
    return Promise.all(
      event.Records.map(async (record) => {
        const { bucketName, s3MessageKey } = getS3MessageKeyAndBucket(
          record.messageAttributes?.[S3_MESSAGE_BODY_KEY]?.stringValue,
        );
        if (s3MessageKey) {
          await this._deleteS3Content(bucketName, s3MessageKey);
        }
      }),
    );
  }
}

function getMessageAttributesSize(messageAttributes: { [key: string]: MessageAttributeValue }) {
  if (!messageAttributes) {
    return 0;
  }

  let size = 0;

  Object.keys(messageAttributes).forEach((attrKey) => {
    const attr = messageAttributes[attrKey];

    size += Buffer.byteLength(attrKey, 'utf8');
    size += attr.DataType ? Buffer.byteLength(attr.DataType, 'utf8') : 0;
    size +=
      typeof attr.StringValue !== 'undefined' && attr.StringValue !== null
        ? Buffer.byteLength(attr.StringValue, 'utf8')
        : 0;
    size +=
      typeof attr.BinaryValue !== 'undefined' && attr.BinaryValue !== null
        ? Buffer.byteLength(attr.BinaryValue, 'utf8')
        : 0;
  });

  return size;
}

function isLarge(message: SendMessageRequest, messageSizeThreshold = DEFAULT_MESSAGE_SIZE_THRESHOLD) {
  const messageAttributeSize = message.MessageAttributes ? getMessageAttributesSize(message.MessageAttributes) : 0;
  const bodySize = message.MessageBody ? Buffer.byteLength(message.MessageBody, 'utf8') : 0;
  return messageAttributeSize + bodySize > messageSizeThreshold;
}

function defaultSendTransform(alwaysUseS3?: boolean, messageSizeThreshold?: number) {
  return (message: SendMessageRequest) => {
    const useS3 = alwaysUseS3 || isLarge(message, messageSizeThreshold);

    return {
      messageBody: useS3 ? null : message.MessageBody,
      s3Content: useS3 ? message.MessageBody : null,
    };
  };
}

function defaultReceiveTransform() {
  return (message: Message, s3Content: string) => {
    return s3Content || message.Body;
  };
}

function getS3MessageKeyAndBucket(s3MessageKey: string | undefined) {
  if (!s3MessageKey) {
    return {
      bucketName: null,
      s3MessageKey: null,
    };
  }

  const s3MessageKeyRegexMatch = s3MessageKey.match(/^\((.*)\)(.*)?/);
  if (s3MessageKeyRegexMatch) {
    return {
      bucketName: s3MessageKeyRegexMatch[1],
      s3MessageKey: s3MessageKeyRegexMatch[2],
    };
  } else {
    throw new Error(`Can not found match s3MessageKeyRegexMatch`);
  }
}

function embedS3MarkersInReceiptHandle(bucketName: string, s3MessageKey: string, receiptHandle?: string) {
  return `${S3_BUCKET_NAME_MARKER}${bucketName}${S3_BUCKET_NAME_MARKER}${S3_MESSAGE_KEY_MARKER}${s3MessageKey}${S3_MESSAGE_KEY_MARKER}${receiptHandle}`;
}

function addS3MessageKeyAttribute(
  s3MessageKey: string,
  attributes: {
    [key: string]: MessageAttributeValue;
  },
) {
  return {
    ...attributes,
    [S3_MESSAGE_BODY_KEY]: {
      DataType: 'String',
      StringValue: s3MessageKey,
    },
  };
}

function extractBucketNameFromReceiptHandle(receiptHandle: string) {
  if (receiptHandle.indexOf(S3_BUCKET_NAME_MARKER) >= 0) {
    return receiptHandle.substring(
      receiptHandle.indexOf(S3_BUCKET_NAME_MARKER) + S3_BUCKET_NAME_MARKER.length,
      receiptHandle.lastIndexOf(S3_BUCKET_NAME_MARKER),
    );
  }

  return null;
}

function extractS3MessageKeyFromReceiptHandle(receiptHandle: string) {
  if (receiptHandle.indexOf(S3_MESSAGE_KEY_MARKER) >= 0) {
    return receiptHandle.substring(
      receiptHandle.indexOf(S3_MESSAGE_KEY_MARKER) + S3_MESSAGE_KEY_MARKER.length,
      receiptHandle.lastIndexOf(S3_MESSAGE_KEY_MARKER),
    );
  }

  return null;
}

function getOriginReceiptHandle(receiptHandle: string) {
  return receiptHandle.indexOf(S3_MESSAGE_KEY_MARKER) >= 0
    ? receiptHandle.substring(receiptHandle.lastIndexOf(S3_MESSAGE_KEY_MARKER) + S3_MESSAGE_KEY_MARKER.length)
    : receiptHandle;
}

function wrapRequest(
  request: any,
  callback: callbackFnType | undefined,
  sendFn: preRequestFuncType | postRequestFuncType,
) {
  if (callback) {
    sendFn(callback);
  }

  return {
    ...request,
    send: sendFn,
    promise: sendFn,
  };
}

function invokeFnBeforeRequest(request: any, fn: preRequestFuncType) {
  return (callback: callbackFnType) =>
    new Promise<void>((resolve, reject) => {
      fn()
        .then(() => {
          request
            .then((response: any) => {
              if (callback) {
                callback(undefined, response);
              }

              resolve(response);
            })
            .catch((err: Error) => {
              if (callback) {
                callback(err, request);
                resolve();
                return;
              }

              reject(err);
            });
        })
        .catch((fnErr: Error) => {
          if (callback) {
            callback(fnErr, request);
            resolve();
            return;
          }

          reject(fnErr);
        });
    });
}

function invokeFnAfterRequest(request: any, fn: postRequestFuncType) {
  return (callback: callbackFnType) =>
    new Promise<void>((resolve, reject) => {
      request
        .then((response: any) => {
          fn(response)
            .then(() => {
              if (callback) {
                callback(undefined, response);
              }

              resolve(response);
            })
            .catch((s3Err: Error) => {
              if (callback) {
                callback(s3Err, response);
                resolve();
                return;
              }

              reject(s3Err);
            });
        })
        .catch((err: Error) => {
          if (callback) {
            callback(err);
            resolve();
            return;
          }

          reject(err);
        });
    });
}
