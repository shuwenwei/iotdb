/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InsertRows extends WrappedInsertStatement {

  public InsertRows(InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
    super(insertRowsStatement, context);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRows(this, context);
  }

  @Override
  public InsertRowsStatement getInnerTreeStatement() {
    return ((InsertRowsStatement) super.getInnerTreeStatement());
  }

  @Override
  public void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException {
    getInnerTreeStatement().updateAfterSchemaValidation(context);
  }

  @Override
  public String getDatabase() {
    return context.getSession().getDatabaseName().orElse(null);
  }

  @Override
  public String getTableName() {
    return getInnerTreeStatement().getDevicePath().getFullPath();
  }

  @Override
  public List<Object[]> getDeviceIdList() {
    final InsertRowsStatement insertRowStatement = getInnerTreeStatement();
    return insertRowStatement.getDeviceIdList();
  }

  @Override
  public List<String> getAttributeColumnNameList() {
    // each row may have different columns
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Object[]> getAttributeValueList() {
    // each row may have different columns
    throw new UnsupportedOperationException();
  }

  @Override
  public void validateTableSchema(TableSchema realSchema) {
    for (InsertRowStatement insertRowStatement :
        getInnerTreeStatement().getInsertRowStatementList()) {
      final TableSchema incomingTableSchema = toTableSchema(insertRowStatement);
      validateTableSchema(realSchema, incomingTableSchema, insertRowStatement);
    }
  }

  @Override
  public void validateDeviceSchema(Metadata metadata, MPPQueryContext context) {
    for (InsertRowStatement insertRowStatement :
        getInnerTreeStatement().getInsertRowStatementList()) {
      metadata.validateDeviceSchema(createTableSchemaValidation(insertRowStatement), context);
    }
  }

  protected ITableDeviceSchemaValidation createTableSchemaValidation(
      InsertRowStatement insertRowStatement) {
    return new ITableDeviceSchemaValidation() {

      @Override
      public String getDatabase() {
        return InsertRows.this.getDatabase();
      }

      @Override
      public String getTableName() {
        return InsertRows.this.getTableName();
      }

      @Override
      public List<Object[]> getDeviceIdList() {
        return Collections.singletonList(insertRowStatement.getTableDeviceID().getSegments());
      }

      @Override
      public List<String> getAttributeColumnNameList() {
        List<String> attributeColumnNameList = new ArrayList<>();
        for (int i = 0; i < insertRowStatement.getColumnCategories().length; i++) {
          if (insertRowStatement.getColumnCategories()[i] == TsTableColumnCategory.ATTRIBUTE) {
            attributeColumnNameList.add(insertRowStatement.getMeasurements()[i]);
          }
        }
        return attributeColumnNameList;
      }

      @Override
      public List<Object[]> getAttributeValueList() {
        List<Object> attributeValueList = new ArrayList<>();
        for (int i = 0; i < insertRowStatement.getColumnCategories().length; i++) {
          if (insertRowStatement.getColumnCategories()[i] == TsTableColumnCategory.ATTRIBUTE) {
            attributeValueList.add(insertRowStatement.getValues()[i]);
          }
        }
        return Collections.singletonList(attributeValueList.toArray());
      }
    };
  }
}