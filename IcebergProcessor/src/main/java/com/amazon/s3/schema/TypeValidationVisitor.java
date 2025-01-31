/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.amazon.s3.schema;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

/**
 * The metadata conversion code only handles a small subset of the SQL type system.  In particular, it only supports
 * STRING, BOOLEAN, and INTEGER scalar types and the ARRAY container type.  This visitor is used to validate that the
 * custom schema configuration at least pays lip-service to this restriction.
 */
class TypeValidationVisitor extends LogicalTypeDefaultVisitor<Boolean> {

    @Override
    public Boolean visit(BooleanType booleanType) {
        return true;
    }

    @Override
    public Boolean visit(IntType intType) {
        return true;
    }

    @Override
    public Boolean visit(VarCharType varCharType) {
        return true;
    }

    @Override
    public Boolean visit(ArrayType arrayType) {
        return arrayType.getElementType().accept(this);
    }

    @Override
    protected Boolean defaultMethod(LogicalType logicalType) {
        throw new ValidationException("Unsupported data type: " + logicalType.toString());
    }
}
