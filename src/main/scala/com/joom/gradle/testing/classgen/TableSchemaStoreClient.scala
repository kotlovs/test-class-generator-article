package com.joom.gradle.testing.classgen

import java.io.Closeable
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.types.{DataType, StructType}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

/**
 * Retrieves schemas for Spark tables from Hive Metastore using PlatformManager api.
 *
 * @param tableSchemaStoreUrl  the schemastore url (default: http://schemastore.joom.a/api/tables)
 */
class TableSchemaStoreClient(tableSchemaStoreUrl: String = TableSchemaStoreClient.defaultTableSchemaStoreUrl)
  extends Closeable {

  private val httpClient = HttpClientBuilder.create.build

  def getSparkSchema(database: String, table: String): StructType = {
    val url = s"$tableSchemaStoreUrl/$database.$table"
    val get = new HttpGet(url)
    get.addHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON.getMimeType)

    using(httpClient.execute(get)) { response =>
      val responseBody = IOUtils.toString(response.getEntity.getContent, StandardCharsets.UTF_8)

      if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK)
        throw new Exception(s"Schema request failed: $responseBody. Check that table with name $database.$table exists.")

      val s = parse(responseBody) \\ "schema"
      DataType.fromJson(compact(render(s))).asInstanceOf[StructType]
    }
  }

  override def close(): Unit = {
    if (httpClient != null)
      httpClient.close()
  }
}

object TableSchemaStoreClient {
  val defaultTableSchemaStoreUrl = "http://schemastore.joom.a/api/tables"
}
