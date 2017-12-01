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

import java.nio.charset.CharacterCodingException
import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.model.Record

/**
 * Processes records and checkpoints progress.
 */
class AmazonKinesisApplicationSampleRecordProcessor : IRecordProcessor {
    private var kinesisShardId: String? = null
    private var nextCheckpointTimeInMillis: Long = 0

    private val decoder = Charset.forName("UTF-8").newDecoder()

    /**
     * {@inheritDoc}
     */

    override fun initialize(initializationInput: InitializationInput) {
        LOG.info("Initializing record processor for shard: " + initializationInput.shardId)
        this.kinesisShardId = initializationInput.shardId
    }

    /**
     * {@inheritDoc}
     */

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        LOG.info("Processing " + processRecordsInput.records.size + " records from " + kinesisShardId)

        // Process records and perform all exception handling.
        processRecordsWithRetries(processRecordsInput.records)

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(processRecordsInput.checkpointer)
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.

     * @param records Data records to be processed.
     */
    private fun processRecordsWithRetries(records: List<Record>) {
        for (record in records) {
            var processedSuccessfully = false
            for (i in 0..NUM_RETRIES - 1) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    processSingleRecord(record)

                    processedSuccessfully = true
                    break
                } catch (t: Throwable) {
                    LOG.warn("Caught throwable while processing record " + record, t)
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS)
                } catch (e: InterruptedException) {
                    LOG.debug("Interrupted sleep", e)
                }

            }

            if (!processedSuccessfully) {
                LOG.error("Couldn't process record $record. Skipping the record.")
            }
        }
    }

    /**
     * Process a single record.

     * @param record The record to be processed.
     */
    private fun processSingleRecord(record: Record) {
        // TODO Add your own record processing logic here

        var data: String? = null
        try {
            // For this app, we interpret the payload as UTF-8 chars.
            data = decoder.decode(record.data).toString()
            // Assume this record came from AmazonKinesisSample and log its age.
            val recordCreateTime = java.lang.Long.valueOf(data.subSequence("testData-".length, data.length).toString())
            val ageOfRecordInMillis = System.currentTimeMillis() - recordCreateTime

            LOG.info(record.sequenceNumber + ", " + record.partitionKey + ", " + data + ", Created "
                    + ageOfRecordInMillis + " milliseconds ago.")
        } catch (e: NumberFormatException) {
            LOG.info("Record does not match sample record format. Ignoring record with data; " + data!!)
        } catch (e: CharacterCodingException) {
            LOG.error("Malformed data: " + data!!, e)
        }

    }

    /**
     * {@inheritDoc}
     */

    override fun shutdown(shutdownInput: ShutdownInput) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId!!)
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownInput.shutdownReason == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.checkpointer)
        }
    }

    /** Checkpoint with retries.
     * @param checkpointer
     */
    private fun checkpoint(checkpointer: IRecordProcessorCheckpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId!!)
        for (i in 0..NUM_RETRIES - 1) {
            try {
                checkpointer.checkpoint()
                break
            } catch (se: ShutdownException) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se)
                break
            } catch (e: ThrottlingException) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= NUM_RETRIES - 1) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e)
                    break
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e)
                }
            } catch (e: InvalidStateException) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e)
                break
            }

            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS)
            } catch (e: InterruptedException) {
                LOG.debug("Interrupted sleep", e)
            }

        }
    }

    companion object {

        private val LOG = LogFactory.getLog(AmazonKinesisApplicationSampleRecordProcessor::class.java)

        // Backoff and retry settings
        private val BACKOFF_TIME_IN_MILLIS = 3000L
        private val NUM_RETRIES = 10

        // Checkpoint about once a minute
        private val CHECKPOINT_INTERVAL_MILLIS = 60000L
    }
}
