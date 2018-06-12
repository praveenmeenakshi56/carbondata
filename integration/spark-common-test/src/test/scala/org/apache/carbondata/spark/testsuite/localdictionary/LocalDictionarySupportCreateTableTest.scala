package org.apache.carbondata.spark.testsuite.localdictionary

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class LocalDictionarySupportCreateTableTest extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")
  }

  test("test local dictionary default configuration") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test("test local dictionary custom configurations for local dict columns _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_include'='name')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations for local dict columns _002")
  {
    sql("drop table if exists local1")

    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations for local dict columns _003") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_include'='')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICT_INCLUDE/LOCAL_DICT_EXCLUDE column:  does not exist in table. Please check create table statement"))

  }


  test("test local dictionary custom configurations for local dict columns _004") {
    sql("drop table if exists local1")
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_include'='abc')
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICT_INCLUDE/LOCAL_DICT_EXCLUDE column: abc does not exist in table. Please check create table " +
        "statement"))
  }

  test("test local dictionary custom configurations for local dict columns _005") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_include'='id')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICT_INCLUDE/LOCAL_DICT_EXCLUDE column: id is not a String datatype column. LOCAL_DICT_COLUMN should " +
        "be no dictionary string datatype column"))
  }

  test("test local dictionary custom configurations for local dict columns _006") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('dictionary_include'='name','local_dict_include'='name')
        """.
          stripMargin)
    }
  }

  test("test local dictionary custom configurations for local dict threshold _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='10000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test(
    "test local dictionary custom configurations for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='-100')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test(
    "test local dictionary custom configurations for local dict threshold _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='21474874811')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test(
    "test local dictionary custom configurations for local dict threshold _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test(
    "test local dictionary custom configurations for local dict threshold _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='10000','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    sql("desc formatted local1").show(truncate = false)
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='-100','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    sql("desc formatted local1").show(truncate = false)
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }


  test("test local dictionary custom configurations with both columns and threshold configured _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    sql("desc formatted local1").show(truncate = false)
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='vdslv','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    sql("desc formatted local1").show(truncate = false)
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _005") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_threshold'='10000','local_dict_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _006") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_threshold'='10000','local_dict_include'=' ')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _007") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_threshold'='10000','local_dict_include'='hello')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _008") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_threshold'='10000','local_dict_include'='name','dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _009") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_threshold'='','local_dict_include'='name,name',)
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _010") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_threshold'='-100','local_dict_include'='Hello')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations with both columns and threshold configured _011") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_threshold'='23213497321591234324','local_dict_include'='name','dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary default configuration when enabled") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dict_enable'='true')
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test("test local dictionary custom configurations when enabled for local dict columns _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_include'='name','local_dict_enable'='true')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled for local dict columns _002")
  {
    sql("drop table if exists local1")

    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled for local dict columns _003") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_include'='')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICT_INCLUDE/LOCAL_DICT_EXCLUDE column:  does not exist in table. Please check create table statement"))

  }

  test("test local dictionary custom configurations when enabled for local dict columns _004") {
    sql("drop table if exists local1")
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_include'='abc')
        """.
          stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "LOCAL_DICT_INCLUDE/LOCAL_DICT_EXCLUDE column: abc does not exist in table. Please check create table " +
        "statement"))
  }

  test("test local dictionary custom configurations when enabled for local dict columns _005") {
    sql("drop table if exists local1")
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_include'='id')
        """.
          stripMargin)
    }
    assert(exception.getMessage
      .contains(
        "LOCAL_DICT_INCLUDE/LOCAL_DICT_EXCLUDE column: id is not a String datatype column. LOCAL_DICT_COLUMN should " +
        "be no dictionary string datatype column"))
  }

  test("test local dictionary custom configurations when enabled for local dict columns _006") {
    sql("drop table if exists local1")
     intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','dictionary_include'='name','local_dict_include'='name')
        """.
          stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled for local dict threshold _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='true','local_dict_threshold'='10000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='true','local_dict_threshold'='-100')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled for local dict threshold _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='true','local_dict_threshold'='21474874811')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled for local dict threshold _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='true','local_dict_threshold'='')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test(
    "test local dictionary custom configurations when enabled for local dict threshold _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='true','local_dict_threshold'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='true','local_dict_threshold'='10000','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    sql("desc formatted local1").show(truncate = false)
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='true','local_dict_threshold'='-100','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    sql("desc formatted local1").show(truncate = false)
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='true','local_dict_threshold'='','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    sql("desc formatted local1").show(truncate = false)
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='true','local_dict_threshold'='vdslv','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    sql("desc formatted local1").show(truncate = false)
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("name"))
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _005") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_threshold'='10000','local_dict_include'='name,name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _006") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_threshold'='10000','local_dict_include'=' ')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _007") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_threshold'='10000','local_dict_include'='hello')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _008") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_threshold'='10000','local_dict_include'='name','dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _009") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_threshold'='','local_dict_include'='name,name',)
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _010") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_threshold'='-100','local_dict_include'='Hello')
        """.stripMargin)
    }
  }

  test("test local dictionary custom configurations when enabled with both columns and threshold configured _011") {
    sql("drop table if exists local1")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='true','local_dict_threshold'='23213497321591234324','local_dict_include'='name','dictionary_include'='name')
        """.stripMargin)
    }
  }

  test("test local dictionary default configuration when disabled") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('local_dict_enable'='false')
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
          }
    }

  test("test local dictionary custom configurations when disabled for local dict columns _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_include'='name','local_dict_enable'='false')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled for local dict columns _002")
  {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_include'='name,name')
        """.stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _003") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_include'='')
        """.
          stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _004") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_include'='abc')
        """.
          stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _005") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_include'='id')
        """.
          stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict columns _006") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','dictionary_include'='name','local_dict_include'='name')
        """.
          stripMargin)
    val descFormatted1 = sql("describe formatted local1").collect
    descFormatted1.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled for local dict threshold _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_threshold'='10000')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled for local dict threshold _002")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_threshold'='-100')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled for local dict threshold _003")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_threshold'='21474874811')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled for local dict threshold _004")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_threshold'='')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test(
    "test local dictionary custom configurations when disabled for local dict threshold _005")
  {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_threshold'='hello')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _001") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_threshold'='10000','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _002") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_threshold'='-100','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _003") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_threshold'='','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _004") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_threshold'='vdslv','local_dict_include'='name')
      """.stripMargin)

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _005") {
    sql("drop table if exists local1")

      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_threshold'='10000','local_dict_include'='name,name')
        """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _006") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_threshold'='10000','local_dict_include'=' ')
        """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _007") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_threshold'='10000','local_dict_include'='hello')
        """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _008") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_threshold'='10000','local_dict_include'='name','dictionary_include'='name')
        """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _009") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_threshold'='','local_dict_include'='name,name')
        """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _010") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_threshold'='-100','local_dict_include'='Hello')
        """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configurations when disabled with both columns and threshold configured _011") {
    sql("drop table if exists local1")
      sql(
        """
          | CREATE TABLE local1(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_enable'='false','local_dict_threshold'='23213497321591234324','local_dict_include'='name','dictionary_include'='name')
        """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
  }

  test("test local dictionary custom configuration with other table properties") {
    sql("drop table if exists local1")
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('dictionary_include'='city','sort_scope'='global_sort','sort_columns'='city,name')
      """.stripMargin)
    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("SORT_SCOPE")) match {
      case Some(row) => assert(row.get(1).toString.contains("global_sort"))
    }
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")

  }
}
