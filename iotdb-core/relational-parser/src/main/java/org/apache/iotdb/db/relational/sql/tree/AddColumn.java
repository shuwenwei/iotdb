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

package org.apache.iotdb.db.relational.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AddColumn extends Statement {

  private final QualifiedName tableName;
  private final ColumnDefinition column;

  public AddColumn(QualifiedName tableName, ColumnDefinition column) {
    super(null);

    this.tableName = requireNonNull(tableName, "tableName is null");
    this.column = requireNonNull(column, "column is null");
  }

  public AddColumn(NodeLocation location, QualifiedName tableName, ColumnDefinition column) {
    super(requireNonNull(location, "location is null"));

    this.tableName = requireNonNull(tableName, "tableName is null");
    this.column = requireNonNull(column, "column is null");
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public ColumnDefinition getColumn() {
    return column;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAddColumn(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(column);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, column);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    AddColumn o = (AddColumn) obj;
    return Objects.equals(tableName, o.tableName) && Objects.equals(column, o.column);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("name", tableName).add("column", column).toString();
  }
}
