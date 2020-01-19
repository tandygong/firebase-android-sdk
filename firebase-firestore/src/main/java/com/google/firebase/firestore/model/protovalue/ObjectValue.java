// Copyright 2020 Google LLC
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

package com.google.firebase.firestore.model.protovalue;

import static com.google.firebase.firestore.util.Assert.hardAssert;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import com.google.firebase.database.collection.ImmutableSortedMap;
import com.google.firebase.firestore.model.FieldPath;
import com.google.firebase.firestore.util.Util;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class ObjectValue extends FieldValue implements Iterable<Map.Entry<String, FieldValue>> {
  private final MapValue internalValue;
  private final ImmutableSortedMap<String, FieldValue> overlays;

  public ObjectValue(MapValue backingValue, ImmutableSortedMap<String, FieldValue> overlays) {
    this.internalValue = backingValue;
    this.overlays = overlays;
  }

  public ObjectValue() {
    this(MapValue.newBuilder().build());
  }

  public ObjectValue(MapValue backingValue) {
    this(backingValue, ImmutableSortedMap.Builder.emptyMap(String::compareTo));
  }

  @Override
  public int typeOrder() {
    return TYPE_ORDER_OBJECT;
  }

  @Nullable
  @Override
  public Object value() {
    Map<String, Object> result = new HashMap<>();
    for (Map.Entry<String, FieldValue> entry : this) {
      result.put(entry.getKey(), entry.getValue().value());
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ObjectValue) {
      Iterator<Map.Entry<String, FieldValue>> iterator1 = this.iterator();
      Iterator<Map.Entry<String, FieldValue>> iterator2 = ((ObjectValue) o).iterator();
      while (iterator1.hasNext() && iterator2.hasNext()) {
        Map.Entry<String, FieldValue> entry1 = iterator1.next();
        Map.Entry<String, FieldValue> entry2 = iterator2.next();
        if (entry1.equals(entry2)) {
          return false;
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 0;
    for (Map.Entry<String, FieldValue> entry : this) {
      hashCode = hashCode * 31 + entry.getKey().hashCode();
      hashCode = hashCode * 31 + entry.getValue().hashCode();
    }
    return hashCode;
  }

  @Override
  public int compareTo(@NonNull FieldValue o) {
    if (!(o instanceof ObjectValue)) {
      return Util.compareIntegers(typeOrder(), o.typeOrder());
    }

    Iterator<Map.Entry<String, FieldValue>> iterator1 = this.iterator();
    Iterator<Map.Entry<String, FieldValue>> iterator2 = ((ObjectValue) o).iterator();
    while (iterator1.hasNext() && iterator2.hasNext()) {
      Map.Entry<String, FieldValue> entry1 = iterator1.next();
      Map.Entry<String, FieldValue> entry2 = iterator2.next();
      int keyCompare = entry1.getKey().compareTo(entry2.getKey());
      if (keyCompare != 0) {
        return keyCompare;
      }
      int valueCompare = entry1.getValue().compareTo(entry2.getValue());
      if (valueCompare != 0) {
        return valueCompare;
      }
    }

    // Only equal if both iterators are exhausted.
    return Util.compareBooleans(iterator1.hasNext(), iterator2.hasNext());
  }

  public ObjectValue set(FieldPath path, FieldValue value) {
    hardAssert(!path.isEmpty(), "Cannot set field for empty path on ObjectValue");
    String childName = path.getFirstSegment();
    if (path.length() == 1) {
      return setChild(childName, value);
    } else {
      FieldValue child = get(childName);
      if (!(child instanceof ObjectValue)) {
        child = new ObjectValue();
      }
      child = ((ObjectValue) child).set(path.popFirst(), value);
      return setChild(childName, child);
    }
  }

  public ObjectValue delete(FieldPath path) {
    hardAssert(!path.isEmpty(), "Cannot delete field for empty path on ObjectValue");
    String childName = path.getFirstSegment();
    if (path.length() == 1) {
      return new ObjectValue(internalValue, overlays.insert(childName, null));
    } else {
      FieldValue child = get(childName);
      if (child instanceof ObjectValue) {
        ObjectValue newChild = ((ObjectValue) child).delete(path.popFirst());
        return setChild(childName, newChild);
      } else {
        // Don't actually change a primitive value to an object for a delete.
        return this;
      }
    }
  }

  private @javax.annotation.Nullable FieldValue get(String childName) {
    if (overlays.containsKey(childName)) {
      return overlays.get(childName);
    } else {
      Value value = this.internalValue.getFieldsMap().get(childName);
      if (value == null) {
        return null;
      }
      if (value.getValueTypeCase() == Value.ValueTypeCase.MAP_VALUE) {
        return new ObjectValue(value.getMapValue());
      }
      return new PrimitiveValue(value);
    }
  }

  private ObjectValue setChild(String childName, FieldValue value) {
    return new ObjectValue(this.internalValue, overlays.insert(childName, value));
  }

  @NonNull
  @Override
  public Iterator<Map.Entry<String, FieldValue>> iterator() {
    return new Iterator<Map.Entry<String, FieldValue>>() {
      Iterator<Map.Entry<String, Value>> valueIterator =
          internalValue.getFieldsMap().entrySet().iterator();
      Map.Entry<String, Value> valuePeek = null;
      Iterator<Map.Entry<String, FieldValue>> overlayIterator = overlays.iterator();
      Map.Entry<String, FieldValue> overlayPeek = null;

      private void peek() {
        if (valuePeek == null && valueIterator.hasNext()) {
          valuePeek = valueIterator.next();
        }

        if (overlayPeek == null && overlayIterator.hasNext()) {
          overlayPeek = overlayIterator.next();
        }

        if (overlayPeek != null && overlayPeek.getValue() == null) {
          if (valuePeek.getKey().equals(overlayPeek.getKey())) {
            valuePeek = null;
          }

          overlayPeek = null;
          peek();
        }
      }

      @Override
      public boolean hasNext() {
        peek();
        return valuePeek != null || overlayPeek != null;
      }

      @Override
      public Map.Entry<String, FieldValue> next() {
        peek();

        if (valuePeek != null && overlayPeek != null) {
          int keyCompare = valuePeek.getKey().compareTo(overlayPeek.getKey());

          Map.Entry<String, FieldValue> result;
          if (keyCompare < 0) {
            result = createMapEntry(valuePeek.getKey(), valuePeek.getValue());
            valuePeek = null;
          } else if (keyCompare == 0) {
            result = overlayPeek;
            overlayPeek = null;
            valuePeek = null;
          } else {
            result = overlayPeek;
            overlayPeek = null;
          }
          return result;
        } else if (valuePeek != null) {
          return createMapEntry(valuePeek.getKey(), valuePeek.getValue());
        } else if (overlayPeek != null) {
          return overlayPeek;
        }

        throw new NoSuchElementException();
      }
    };
  }

  private Map.Entry<String, FieldValue> createMapEntry(String key, Value value) {
    return new Map.Entry<String, FieldValue>() {
      @Override
      public String getKey() {
        return key;
      }

      @Override
      public FieldValue getValue() {
        return new PrimitiveValue(value);
      }

      @Override
      public FieldValue setValue(FieldValue value) {
        throw new UnsupportedOperationException("Not supported");
      }
    };
  }
}
