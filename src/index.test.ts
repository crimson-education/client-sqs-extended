import { mockClient } from 'aws-sdk-client-mock';
import { SQS, SendMessageCommand, ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { GetObjectCommand, PutObjectCommand, S3 } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import { SQSExtended } from './index';

const mockS3Key = '1234-5678';

jest.mock('uuid', () => ({
  v4: () => mockS3Key,
}));

const testMessageAttribute = {
  DataType: 'String',
  StringValue: 'attr value',
};
const s3MessageBodyKeyAttribute = {
  DataType: 'String',
  StringValue: `(test-bucket)${mockS3Key}`,
};

describe('SQSExtended sendMessage', () => {
  it('should send small message directly', async () => {
    // Given
    const sqs = new SQS({});
    const s3 = new S3({});
    const sqsMock = mockClient(sqs);

    const client = new SQSExtended(sqs, s3, { bucketName: 'test-bucket' });

    // When
    await client.sendMessage({
      QueueUrl: 'test-queue',
      MessageAttributes: { MessageAttribute: testMessageAttribute },
      MessageBody: 'small message body',
    });

    // Then
    expect(sqsMock.calls().length).toEqual(1);
    expect(sqsMock.call(0).args[0].input).toEqual({
      QueueUrl: 'test-queue',
      MessageBody: 'small message body',
      MessageAttributes: { MessageAttribute: testMessageAttribute },
    });
  });

  it('should use S3 to send large message', async () => {
    // Given
    const sqsResponse = { MessageId: 'test message id' };

    const sqs = new SQS({});
    const s3 = new S3({});
    const sqsMock = mockClient(sqs);
    const s3Mock = mockClient(s3);

    sqsMock.on(SendMessageCommand).resolves(sqsResponse);
    s3Mock.on(PutObjectCommand).resolves({});

    const client = new SQSExtended(sqs, s3, { bucketName: 'test-bucket' });
    const largeMessageBody = 'body'.repeat(1024 * 1024);

    // When
    const result = await client.sendMessage({
      QueueUrl: 'test-queue',
      MessageAttributes: { MessageAttribute: testMessageAttribute },
      MessageBody: largeMessageBody,
    });

    // Then
    expect(result).toEqual(sqsResponse);

    expect(s3Mock.calls().length).toEqual(1);
    expect(s3Mock.call(0).args[0].input).toEqual({
      Bucket: 'test-bucket',
      Key: mockS3Key,
      Body: largeMessageBody,
    });

    expect(sqsMock.calls().length).toEqual(1);
    expect(sqsMock.calls()[0].args[0].input).toEqual({
      QueueUrl: 'test-queue',
      MessageBody: mockS3Key,
      MessageAttributes: {
        MessageAttribute: testMessageAttribute,
        S3MessageBodyKey: s3MessageBodyKeyAttribute,
      },
    });
  });
});

describe('SQSExtended receiveMessage', () => {
  it('should receive a message not using S3', async () => {
    // Given
    const messages = {
      Messages: [
        {
          Body: 'message body',
          ReceiptHandler: 'receipthandle',
          MessageAttributes: {
            MessageAttribute: testMessageAttribute,
          },
        },
      ],
    };
    const sqs = new SQS({});
    const s3 = new S3({});
    const sqsMock = mockClient(sqs);
    sqsMock.on(ReceiveMessageCommand).resolves(messages);

    const client = new SQSExtended(sqs, s3, {});

    // When
    const response = await client.receiveMessage({
      QueueUrl: 'test-queue',
      MessageAttributeNames: ['MessageAttribute'],
    });

    // Then
    expect(sqsMock.calls().length).toEqual(1);
    expect(sqsMock.call(0).args[0].input).toEqual({
      MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
      QueueUrl: 'test-queue',
    });

    expect(response).toEqual(messages);
  });

  it('should receive a message using S3', async () => {
    // Given
    const messages = {
      Messages: [
        {
          Body: '8765-4321',
          ReceiptHandle: 'receipthandle',
          MessageAttributes: {
            MessageAttribute: testMessageAttribute,
            S3MessageBodyKey: s3MessageBodyKeyAttribute,
          },
        },
      ],
    };

    const sqs = new SQS({});
    const stream = Readable.from('message body');
    const s3Content = {
      Body: stream,
    };
    const s3 = new S3({});

    const sqsMock = mockClient(sqs);
    const s3Mock = mockClient(s3);
    sqsMock.on(ReceiveMessageCommand).resolves(messages);
    s3Mock.on(GetObjectCommand).resolves(s3Content);

    const client = new SQSExtended(sqs, s3, {});

    // When
    const response = await client.receiveMessage({
      QueueUrl: 'test-queue',
      MessageAttributeNames: ['MessageAttribute'],
    });

    // Then
    expect(sqsMock.calls().length).toEqual(1);
    expect(sqsMock.call(0).args[0].input).toEqual({
      MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
      QueueUrl: 'test-queue',
    });

    expect(s3Mock.calls().length).toEqual(1);
    expect(s3Mock.call(0).args[0].input).toEqual({
      Bucket: 'test-bucket',
      Key: mockS3Key,
    });

    expect(response).toEqual({
      Messages: [
        {
          Body: 'message body',
          ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
          MessageAttributes: {
            MessageAttribute: testMessageAttribute,
            S3MessageBodyKey: s3MessageBodyKeyAttribute,
          },
        },
      ],
    });
  });
});
