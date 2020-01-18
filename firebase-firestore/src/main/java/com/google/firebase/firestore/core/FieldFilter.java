// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.firebase.firestore.core;

import static com.google.firebase.firestore.util.Assert.fail;
import static com.google.firebase.firestore.util.Assert.hardAssert;

import com.google.firebase.firestore.model.Document;
import com.google.firebase.firestore.model.FieldPath;
import com.google.firebase.firestore.util.Assert;
import com.google.firebase.firestore.util.Util;
import com.google.firestore.v1.Value;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/** Represents a filter to be applied to query. */
public class FieldFilter extends Filter {
  static final int TYPE_ORDER_NULL = 0;

  static final int TYPE_ORDER_BOOLEAN = 1;
  static final int TYPE_ORDER_NUMBER = 2;
  static final int TYPE_ORDER_TIMESTAMP = 3;
  static final int TYPE_ORDER_STRING = 4;
  static final int TYPE_ORDER_BLOB = 5;
  static final int TYPE_ORDER_REFERENCE = 6;
  static final int TYPE_ORDER_GEOPOINT = 7;
  static final int TYPE_ORDER_ARRAY = 8;
  static final int TYPE_ORDER_OBJECT = 9;

  private final Operator operator;

  private final Value value;

  private final FieldPath field;

  /**
   * Creates a new filter that compares fields and values. Only intended to be called from
   * Filter.create().
   */
  protected FieldFilter(FieldPath field, Operator operator, Value value) {
    this.field = field;
    this.operator = operator;
    this.value = value;
  }

  public Operator getOperator() {
    return operator;
  }

  @Override
  public FieldPath getField() {
    return field;
  }

  public Value getValue() {
    return value;
  }

  /**
   * Gets a Filter instance for the provided path, operator, and value.
   *
   * <p>Note that if the relation operator is EQUAL and the value is null or NaN, this will return
   * the appropriate NullFilter or NaNFilter class instead of a FieldFilter.
   */
  public static FieldFilter create(FieldPath path, Operator operator, Value value) {
    if (path.isKeyField()) {
      if (operator == Operator.IN) {
        hardAssert(
            value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE,
            "Comparing on key with IN, but the value was not an ArrayValue");
        return new KeyFieldInFilter(path, value);
      } else {
        hardAssert(
            value.getValueTypeCase() == Value.ValueTypeCase.REFERENCE_VALUE,
            "Comparing on key, but filter value not a ReferenceValue");
        hardAssert(
            operator != Operator.ARRAY_CONTAINS && operator != Operator.ARRAY_CONTAINS_ANY,
            operator.toString() + "queries don't make sense on document keys");
        return new KeyFieldFilter(path, operator, value);
      }
    } else if (value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
      if (operator != Filter.Operator.EQUAL) {
        throw new IllegalArgumentException(
            "Invalid Query. Null supports only equality comparisons (via whereEqualTo()).");
      }
      return new FieldFilter(path, operator, value);
    } else if (Double.isNaN(value.getDoubleValue())) {
      if (operator != Filter.Operator.EQUAL) {
        throw new IllegalArgumentException(
            "Invalid Query. NaN supports only equality comparisons (via whereEqualTo()).");
      }
      return new FieldFilter(path, operator, value);
    } else if (operator == Operator.ARRAY_CONTAINS) {
      return new ArrayContainsFilter(path, value);
    } else if (operator == Operator.IN) {
      hardAssert(
          value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE,
          "IN filter has invalid value: " + value.toString());
      return new InFilter(path, value);
    } else if (operator == Operator.ARRAY_CONTAINS_ANY) {
      hardAssert(
          value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE,
          "ARRAY_CONTAINS_ANY filter has invalid value: " + value.toString());
      return new ArrayContainsAnyFilter(path, value);
    } else {
      return new FieldFilter(path, operator, value);
    }
  }

  @Override
  public boolean matches(Document doc) {
    Value other = doc.getField(field);
    // Only compare types with matching backend order (such as double and int).
    return other != null
        && getTypeOrder(value) == getTypeOrder(other)
        && this.matchesComparison(compareValues(other, value));
  }

  public static int getTypeOrder(Value value) {
    switch (value.getValueTypeCase()) {
      case NULL_VALUE:
        return TYPE_ORDER_NULL;
      case BOOLEAN_VALUE:
        return TYPE_ORDER_BOOLEAN;
      case INTEGER_VALUE:
        return TYPE_ORDER_NUMBER;
      case DOUBLE_VALUE:
        return TYPE_ORDER_NUMBER;
      case TIMESTAMP_VALUE:
        return TYPE_ORDER_TIMESTAMP;
      case STRING_VALUE:
        return TYPE_ORDER_STRING;
      case BYTES_VALUE:
        return TYPE_ORDER_BLOB;
      case REFERENCE_VALUE:
        return TYPE_ORDER_REFERENCE;
      case GEO_POINT_VALUE:
        return TYPE_ORDER_GEOPOINT;
      case ARRAY_VALUE:
        return TYPE_ORDER_ARRAY;
      case MAP_VALUE:
        return TYPE_ORDER_OBJECT;
      case VALUETYPE_NOT_SET:
        throw fail("");
    }

    return 0;
  }

  public static int compareValues(Value left, Value right) {
    int leftType = getTypeOrder(left);
    int rightType = getTypeOrder(right);

    if (leftType != rightType) {
      return Util.compareIntegers(leftType, rightType);
    }

    switch (leftType) {
      case TYPE_ORDER_NULL:
        return 0;
      case TYPE_ORDER_BOOLEAN:
        return Util.compareBooleans(left.getBooleanValue(), right.getBooleanValue());
      case TYPE_ORDER_NUMBER:
        if (left.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE) {
          double thisDouble = left.getDoubleValue();
          if (right.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE) {
            return Util.compareDoubles(thisDouble, right.getDoubleValue());
          } else {
            hardAssert(
                right.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE,
                "Unknown NumberValue: %s",
                right);
            return Util.compareMixed(thisDouble, right.getIntegerValue());
          }
        } else {
          hardAssert(
              left.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE,
              "Unknown NumberValue: %s",
              left);
          long thisLong = left.getIntegerValue();
          if (right.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
            return Util.compareLongs(thisLong, right.getIntegerValue());
          } else {
            hardAssert(
                right.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE,
                "Unknown NumberValue: %s",
                right);
            return -1 * Util.compareMixed(left.getDoubleValue(), thisLong);
          }
        }
      case TYPE_ORDER_TIMESTAMP:
        if (left.getTimestampValue().getSeconds() == right.getTimestampValue().getSeconds()) {
          return Integer.signum(
              left.getTimestampValue().getNanos() - right.getTimestampValue().getNanos());
        }
        return Long.signum(
            left.getTimestampValue().getSeconds() - right.getTimestampValue().getSeconds());
      case TYPE_ORDER_STRING:
        return left.getStringValue().compareTo(right.getStringValue());
      case TYPE_ORDER_BLOB:
        return Util.compareByteString(left.getBytesValue(), right.getBytesValue());
      case TYPE_ORDER_REFERENCE:
        return left.getReferenceValue().compareTo(right.getReferenceValue());
      case TYPE_ORDER_GEOPOINT:
        int comparison =
            Util.compareDoubles(
                left.getGeoPointValue().getLatitude(), right.getGeoPointValue().getLatitude());
        if (comparison == 0) {
          return Util.compareDoubles(
              left.getGeoPointValue().getLongitude(), right.getGeoPointValue().getLongitude());
        }
      case TYPE_ORDER_ARRAY:
        int minLength =
            Math.min(left.getArrayValue().getValuesCount(), right.getArrayValue().getValuesCount());
        for (int i = 0; i < minLength; i++) {
          int cmp =
              compareValues(left.getArrayValue().getValues(i), right.getArrayValue().getValues(i));
          if (cmp != 0) {
            return cmp;
          }
        }
        return Util.compareIntegers(
            left.getArrayValue().getValuesCount(), right.getArrayValue().getValuesCount());
      case TYPE_ORDER_OBJECT:
        Iterator<Map.Entry<String, Value>> iterator1 =
            left.getMapValue().getFieldsMap().entrySet().iterator();
        Iterator<Map.Entry<String, Value>> iterator2 =
            right.getMapValue().getFieldsMap().entrySet().iterator();
        while (iterator1.hasNext() && iterator2.hasNext()) {
          Map.Entry<String, Value> entry1 = iterator1.next();
          Map.Entry<String, Value> entry2 = iterator2.next();
          int keyCompare = entry1.getKey().compareTo(entry2.getKey());
          if (keyCompare != 0) {
            return keyCompare;
          }
          int valueCompare = compareValues(entry1.getValue(), entry2.getValue());
          if (valueCompare != 0) {
            return valueCompare;
          }
        }

        // Only equal if both iterators are exhausted.
        return Util.compareBooleans(iterator1.hasNext(), iterator2.hasNext());
      default:
        throw fail("Unexpected value");
    }
  }

  protected boolean matchesComparison(int comp) {
    switch (operator) {
      case LESS_THAN:
        return comp < 0;
      case LESS_THAN_OR_EQUAL:
        return comp <= 0;
      case EQUAL:
        return comp == 0;
      case GREATER_THAN:
        return comp > 0;
      case GREATER_THAN_OR_EQUAL:
        return comp >= 0;
      default:
        throw Assert.fail("Unknown FieldFilter operator: %s", operator);
    }
  }

  public boolean isInequality() {
    return Arrays.asList(
            Operator.LESS_THAN,
            Operator.LESS_THAN_OR_EQUAL,
            Operator.GREATER_THAN,
            Operator.GREATER_THAN_OR_EQUAL)
        .contains(operator);
  }

  @Override
  public String getCanonicalId() {
    // TODO: Technically, this won't be unique if two values have the same description,
    // such as the int 3 and the string "3". So we should add the types in here somehow, too.
    return getField().canonicalString() + getOperator().toString() + getValue().toString();
  }

  @Override
  public String toString() {
    return field.canonicalString() + " " + operator + " " + value;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof FieldFilter)) {
      return false;
    }
    FieldFilter other = (FieldFilter) o;
    return operator == other.operator && field.equals(other.field) && value.equals(other.value);
  }

  @Override
  public int hashCode() {
    int result = 37;
    result = 31 * result + operator.hashCode();
    result = 31 * result + field.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }
}
