package com.thomsonreuters.labs

/*
 * Copyright 2012-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.CreateStreamRequest
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.services.kinesis.model.DescribeStreamResult
import com.amazonaws.services.kinesis.model.ListStreamsRequest
import com.amazonaws.services.kinesis.model.ListStreamsResult
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.model.PutRecordResult
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.kinesis.model.StreamDescription

object AmazonKinesisRecordProducerSample {

    /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (~/.aws/credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    private var kinesis: AmazonKinesis? = null

    @Throws(Exception::class)
    private fun init() {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        val credentialsProvider = ProfileCredentialsProvider()
        try {
            credentialsProvider.credentials
        } catch (e: Exception) {
            throw AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e)
        }

        kinesis = AmazonKinesisClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build()
    }

    @Throws(Exception::class)
    @JvmStatic fun main(args: Array<String>) {
        init()

        val myStreamName = AmazonKinesisApplicationSample.SAMPLE_APPLICATION_STREAM_NAME
        val myStreamSize = 1

        // Describe the stream and check if it exists.
        val describeStreamRequest = DescribeStreamRequest().withStreamName(myStreamName)
        try {
            val streamDescription = kinesis!!.describeStream(describeStreamRequest).streamDescription
            System.out.printf("Stream %s has a status of %s.\n", myStreamName, streamDescription.streamStatus)

            if ("DELETING" == streamDescription.streamStatus) {
                println("Stream is being deleted. This sample will now exit.")
                System.exit(0)
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if ("ACTIVE" != streamDescription.streamStatus) {
                waitForStreamToBecomeAvailable(myStreamName)
            }
        } catch (ex: ResourceNotFoundException) {
            System.out.printf("Stream %s does not exist. Creating it now.\n", myStreamName)

            // Create a stream. The number of shards determines the provisioned throughput.
            val createStreamRequest = CreateStreamRequest()
            createStreamRequest.streamName = myStreamName
            createStreamRequest.shardCount = myStreamSize
            kinesis!!.createStream(createStreamRequest)
            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(myStreamName)
        }

        // List all of my streams.
        val listStreamsRequest = ListStreamsRequest()
        listStreamsRequest.limit = 10
        var listStreamsResult = kinesis!!.listStreams(listStreamsRequest)
        val streamNames = listStreamsResult.streamNames
        while (listStreamsResult.isHasMoreStreams) {
            if (streamNames.size > 0) {
                listStreamsRequest.exclusiveStartStreamName = streamNames[streamNames.size - 1]
            }

            listStreamsResult = kinesis!!.listStreams(listStreamsRequest)
            streamNames.addAll(listStreamsResult.streamNames)
        }
        // Print all of my streams.
        println("List of my streams: ")
        for (i in streamNames.indices) {
            println("\t- " + streamNames[i])
        }

        System.out.printf("Putting records in stream : %s until this application is stopped...\n", myStreamName)
        println("Press CTRL-C to stop.")
        // Write records to the stream until this program is aborted.
        while (true) {
            val createTime = System.currentTimeMillis()
            val putRecordRequest = PutRecordRequest()
            putRecordRequest.streamName = myStreamName
            putRecordRequest.data = ByteBuffer.wrap(String.format("testData-%d", createTime).toByteArray())
            putRecordRequest.partitionKey = String.format("partitionKey-%d", createTime)
            val putRecordResult = kinesis!!.putRecord(putRecordRequest)
            System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                    putRecordRequest.partitionKey,
                    putRecordResult.shardId,
                    putRecordResult.sequenceNumber)
        }
    }

    @Throws(InterruptedException::class)
    private fun waitForStreamToBecomeAvailable(myStreamName: String) {
        System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName)

        val startTime = System.currentTimeMillis()
        val endTime = startTime + TimeUnit.MINUTES.toMillis(10)
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20))

            try {
                val describeStreamRequest = DescribeStreamRequest()
                describeStreamRequest.streamName = myStreamName
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.limit = 10
                val describeStreamResponse = kinesis!!.describeStream(describeStreamRequest)

                val streamStatus = describeStreamResponse.streamDescription.streamStatus
                System.out.printf("\t- current state: %s\n", streamStatus)
                if ("ACTIVE" == streamStatus) {
                    return
                }
            } catch (ex: ResourceNotFoundException) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (ase: AmazonServiceException) {
                throw ase
            }

        }

        throw RuntimeException(String.format("Stream %s never became active", myStreamName))
    }
}
