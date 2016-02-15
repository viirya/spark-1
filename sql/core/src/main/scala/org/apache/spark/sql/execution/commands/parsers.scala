/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.commands

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{BaseParser, PlanParser, CatalystQl, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.catalyst.parser.{ASTNode, ParserConf, SimpleParserConf}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.commands._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

case class AlterTableCommandParser(base: CatalystQl) extends PlanParser {

  def parsePartitionSpec(node: ASTNode): Option[Map[String, Option[String]]] = {
    node match {
      case Token("TOK_PARTSPEC", partitions) =>
        val spec = partitions.map {
          case Token("TOK_PARTVAL", ident :: constant :: Nil) =>
            (cleanIdentifier(ident.text), Some(cleanIdentifier(constant.text)))
          case Token("TOK_PARTVAL", ident :: Nil) =>
            (cleanIdentifier(ident.text), None)
        }.toMap
        Some(spec)
      case _ => None
    }
  }

  def extractTableProps(node: ASTNode): Map[String, Option[String]] = node match {
    case Token("TOK_TABLEPROPERTIES", propsList) =>
      propsList.flatMap {
        case Token("TOK_TABLEPROPLIST", props) =>
          props.map {
            case Token("TOK_TABLEPROPERTY", key :: Token("TOK_NULL", Nil) :: Nil) =>
              val k = unquoteString(key.text)
              (k, None)
            case Token("TOK_TABLEPROPERTY", key :: value :: Nil) =>
              val k = unquoteString(key.text)
              val v = unquoteString(value.text)
              (k, Some(v))
          }
      }.toMap
  }

  override def isDefinedAt(node: ASTNode): Boolean = node.text == "TOK_ALTERTABLE"

  override def apply(v1: ASTNode): LogicalPlan = v1.children match {
    case (tabName @ Token("TOK_TABNAME", _)) :: (part @ Token("TOK_PARTSPEC", _)) :: rest =>
      val tableIdent: TableIdentifier = base.extractTableIdent(tabName)
      val partition = parsePartitionSpec(part)
      matchAlterTableCommands(v1, rest, tableIdent, partition)
    case (tabName @ Token("TOK_TABNAME", _)) :: rest =>
      val tableIdent: TableIdentifier = base.extractTableIdent(tabName)
      val partition = None
      matchAlterTableCommands(v1, rest, tableIdent, partition)
    case _ => 
      throw new NotImplementedError(v1.text)
  }

  def matchAlterTableCommands(
      node: ASTNode,
      nodes: Seq[ASTNode],
      tableIdent: TableIdentifier,
      partition: Option[Map[String, Option[String]]]): LogicalPlan = nodes match {
    case rename @ Token("TOK_ALTERTABLE_RENAME", renameArgs) :: rest =>
      val renamedTable = base.getClauseOption("TOK_TABNAME", renameArgs)
      val renamedTableIdent: Option[TableIdentifier] = renamedTable.map(base.extractTableIdent)
      AlterTableRename(tableIdent, renamedTableIdent)(node.source)

    case Token("TOK_ALTERTABLE_PROPERTIES", args) :: rest =>
      val setTableProperties = extractTableProps(args.head)
      AlterTableSetProperties(
        tableIdent,
        setTableProperties)(node.source)

    case Token("TOK_ALTERTABLE_DROPPROPERTIES", args) :: rest =>
      val dropTableProperties = extractTableProps(args.head)
      val allowExisting = base.getClauseOption("TOK_IFEXISTS", args)
      AlterTableDropProperties(
        tableIdent,
        dropTableProperties, allowExisting.isDefined)(node.source)

    case Token("TOK_ALTERTABLE_SERIALIZER", serdeArgs) :: rest =>
      val serdeClassName = serdeArgs.head.text

      val serdeProperties: Option[Map[String, Option[String]]] = Option(
        // SET SERDE serde_classname WITH SERDEPROPERTIES
        if (serdeArgs.tail.isEmpty) {
          null
        } else {
          extractTableProps(serdeArgs.tail.head)
        }
      )
      
      AlterTableSerDeProperties(
        tableIdent,
        Some(serdeClassName),
        serdeProperties,
        partition)(node.source)    

    case Token("TOK_ALTERTABLE_SERDEPROPERTIES", args) :: rest =>
      val serdeProperties: Map[String, Option[String]] = extractTableProps(args.head)

      AlterTableSerDeProperties(
        tableIdent,
        None,
        Some(serdeProperties),
        partition)(node.source)
    /*
    case Token("TOK_ALTERTABLE_CLUSTER_SORT") :: rest =>
    case Token("TOK_ALTERTABLE_BUCKETS") :: rest =>
    case Token("TOK_ALTERTABLE_SKEWED") :: rest =>
    case Token("TOK_ALTERTABLE_SKEWED_LOCATION") :: rest =>
    case Token("TOK_ALTERTABLE_ADDPARTS") :: rest =>
    case Token("TOK_ALTERTABLE_RENAMEPART") :: rest =>
    case Token("TOK_ALTERTABLE_EXCHANGEPARTITION") :: rest =>
    case Token("TOK_ALTERTABLE_DROPPARTS") :: rest =>
    case Token("TOK_ALTERTABLE_ARCHIVE") :: rest =>
    case Token("TOK_ALTERTABLE_UNARCHIVE") :: rest =>
    case Token("TOK_ALTERTABLE_FILEFORMAT") :: rest =>
    case Token("TOK_ALTERTABLE_LOCATION") :: rest =>
    case Token("TOK_ALTERTABLE_TOUCH") :: rest =>
    case Token("TOK_ALTERTABLE_COMPACT") :: rest =>
    case Token("TOK_ALTERTABLE_MERGEFILES") :: rest =>
    case Token("TOK_ALTERTABLE_RENAMECOL") :: rest =>
    case Token("TOK_ALTERTABLE_ADDCOLS") :: rest =>
    case Token("TOK_ALTERTABLE_REPLACECOLS") :: rest =>
    */
  }
}

