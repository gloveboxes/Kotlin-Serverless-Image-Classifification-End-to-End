package com.glovebox.functions

import com.google.gson.stream.JsonReader
import com.microsoft.azure.functions.annotation.*
import com.microsoft.azure.functions.*
import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.azure.functions.annotation.Cardinality
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.CloudTableClient
import com.microsoft.azure.storage.table.TableOperation
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.min
import com.glovebox.models.*


/**
 * Azure Functions with Event Hub trigger.
 */
class ClassifyImage {
    private val _partitionKey: String = System.getenv("PartitionKey")
    private val storageConnectionString = System.getenv("StorageConnectionString")
    private val signalRUrl: String? = System.getenv("AzureSignalRUrl")

    private val storageAccount: CloudStorageAccount = CloudStorageAccount.parse(storageConnectionString)
    private val tableClient: CloudTableClient = storageAccount.createCloudTableClient()
    private val deviceStateTable: CloudTable = getTableReference(tableClient, "ClassifyCount")

    private var top: TableOperation? = null

    // Optimistic Concurrency Tuning Parameters
    private val occBase: Long = 40 // 40 milliseconds
    private val occCap: Long = 1000 // 1000 milliseconds


    /**
     * This function will be invoked when an event is received from Event Hub.
     */
    @FunctionName("ClassifyImage")
    fun run(
            @EventHubTrigger(name = "message", eventHubName = "messages/events", connection = "IotHubConnectionString", consumerGroup = "kotlin-classification-processor", cardinality = Cardinality.ONE) message: List<ImageProbabilityEntity>,
            context: ExecutionContext
    ) {
        val sortedList = message.sortedWith(compareBy { it.probability }).reversed()

        if (sortedList.isNotEmpty()) {
            updateClassityCount(context, sortedList[0])
        }
    }

    private fun updateClassityCount(context: ExecutionContext, item: ImageProbabilityEntity) {
        var maxRetry: Int
        val deviceState = mutableMapOf<String, ImageProbabilityEntity>()

        with (item) {
            context.logger.info("item tag name: $tagName, $probability")
            maxRetry = 0

            while (maxRetry < 10) {
                maxRetry++

                try {
                    top = TableOperation.retrieve(_partitionKey, item.tagName, ImageProbabilityEntity::class.java)
                    val existingEntity = deviceStateTable.execute(top).getResultAsType<ImageProbabilityEntity>()

                    with(item) {
                        partitionKey = _partitionKey
                        rowKey = item.tagName
                        timestamp = Date()
                    }

                    if (existingEntity?.etag != null) {
                        item.etag = existingEntity.etag
                        item.count = existingEntity.count
                        item.count++

                        top = TableOperation.replace(item)
                        deviceStateTable.execute(top)

                    } else {
                        item.count = 1
                        top = TableOperation.insert(item)
                        deviceStateTable.execute(top)
                    }

                    deviceState[item.tagName] = item

                    break

                } catch (e: java.lang.Exception) {
                    val interval = calcExponentialBackoff(maxRetry)
                    Thread.sleep(interval)
                    context.logger.info("Optimistic Consistency Backoff interval $interval")
                }
            }

            if (maxRetry >= 10){
                context.logger.info("Failed to commit")
            }
        }
    }

    private  fun calcExponentialBackoff(attempt: Int) : Long{
        val base = occBase * Math.pow(2.0, attempt.toDouble())
        return ThreadLocalRandom.current().nextLong(occBase,min(occCap, base.toLong()))
    }

    private fun getTableReference(tableClient: CloudTableClient, tableName: String): CloudTable {
        val cloudTable = tableClient.getTableReference(tableName)
        cloudTable.createIfNotExists() // returns true if the table was created
        return cloudTable
    }
}
