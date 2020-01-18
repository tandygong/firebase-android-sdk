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

package com.google.firebase.firestore.model.mutation;

import androidx.annotation.Nullable;
import com.google.firebase.Timestamp;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Value;
import java.util.Collections;
import java.util.List;

/**
 * Base class used for union and remove array transforms.
 *
 * <p>Implementations are: ArrayTransformOperation.Union and ArrayTransformOperation.Remove
 */
public abstract class ArrayTransformOperation implements TransformOperation {
  private final List<Value> elements;

  ArrayTransformOperation(List<Value> elements) {
    this.elements = Collections.unmodifiableList(elements);
  }

  public List<Value> getElements() {
    return elements;
  }

  @Override
  public Value applyToLocalView(@Nullable Value previousValue, Timestamp localWriteTime) {
    return apply(previousValue);
  }

  @Override
  public Value applyToRemoteDocument(@Nullable Value previousValue, Value transformResult) {
    // The server just sends null as the transform result for array operations, so we have to
    // calculate a result the same as we do for local applications.
    return apply(previousValue);
  }

  @Override
  @Nullable
  public Value computeBaseValue(@Nullable Value currentValue) {
    return null; // Array transforms are idempotent and don't require a base value.
  }

  @Override
  @SuppressWarnings("EqualsGetClass") // subtype-sensitive equality is intended.
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArrayTransformOperation that = (ArrayTransformOperation) o;

    return elements.equals(that.elements);
  }

  @Override
  public int hashCode() {
    int result = getClass().hashCode();
    result = 31 * result + elements.hashCode();
    return result;
  }

  /** Applies this ArrayTransformOperation against the specified previousValue. */
  protected abstract Value apply(@Nullable Value previousValue);

  /**
   * Inspects the provided value, returning an ArrayList copy of the internal array if it's an
   * ArrayValue and an empty ArrayList if it's null or any other type of FSTFieldValue.
   */
  static Value coercedFieldValuesArray(@Nullable Value value) {
    if (value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
      return value;
    } else {
      // coerce to empty array.
      return Value.newBuilder()
          .setArrayValue(com.google.firestore.v1.ArrayValue.getDefaultInstance())
          .build();
    }
  }

  /** An array union transform operation. */
  public static class Union extends ArrayTransformOperation {
    public Union(List<Value> elements) {
      super(elements);
    }

    @Override
    protected Value apply(@Nullable Value previousValue) {
      List<Value> result = coercedFieldValuesArray(previousValue).getArrayValue().getValuesList();
      for (Value element : getElements()) {
        if (!result.contains(element)) {
          result.add(element);
        }
      }
      return Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(result)).build();
    }
  }

  /** An array remove transform operation. */
  public static class Remove extends ArrayTransformOperation {
    public Remove(List<Value> elements) {
      super(elements);
    }

    @Override
    protected Value apply(@Nullable Value previousValue) {
      List<Value> result = coercedFieldValuesArray(previousValue).getArrayValue().getValuesList();
      for (Value element : getElements()) {
        result.removeAll(Collections.singleton(element));
      }
      return Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(result)).build();
    }
  }
}
