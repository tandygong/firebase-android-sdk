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
import com.google.firebase.firestore.model.mutation.FieldMask;
import com.google.firebase.firestore.util.SortedMapValueIterator;
import com.google.firebase.firestore.util.Util;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class ObjectValue extends PrimitiveValue implements Iterable<Map.Entry<String, FieldValue>> {
  private final ImmutableSortedMap<String, FieldValue> overlays;

  public ObjectValue(
      @NonNull Value internalValue, @NonNull ImmutableSortedMap<String, FieldValue> overlays) {
    super(internalValue);
    hardAssert(isMap(internalValue), "...");
    this.overlays = overlays;
  }

  public ObjectValue() {
    this(Value.newBuilder().setMapValue(MapValue.getDefaultInstance()).build());
  }

  public ObjectValue(@NonNull Value internalValue) {
    this(internalValue, ImmutableSortedMap.Builder.emptyMap(String::compareTo));
  }

  public static @NonNull ObjectValue emptyObject() {
    return new ObjectValue();
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
    if (o == this) {
      return true;
    }
    if (o instanceof ObjectValue) {
      Iterator<Map.Entry<String, FieldValue>> iterator1 = this.iterator();
      Iterator<Map.Entry<String, FieldValue>> iterator2 = ((ObjectValue) o).iterator();
      while (iterator1.hasNext() && iterator2.hasNext()) {
        Map.Entry<String, FieldValue> entry1 = iterator1.next();
        Map.Entry<String, FieldValue> entry2 = iterator2.next();
        if (!entry1.getKey().equals(entry2.getKey())
            || !entry1.getValue().equals(entry2.getValue())) {
          return false;
        }
      }
      return true;
    } else if (o instanceof PrimitiveValue && isMap(((PrimitiveValue) o).internalValue)) {
      Iterator<Map.Entry<String, FieldValue>> iterator1 = this.iterator();
      Iterator<Map.Entry<String, Value>> iterator2 =
          new SortedMapValueIterator(((PrimitiveValue) o).internalValue);
      while (iterator1.hasNext() && iterator2.hasNext()) {
        Map.Entry<String, FieldValue> entry1 = iterator1.next();
        Map.Entry<String, Value> entry2 = iterator2.next();
        if (entry1.getKey().equals(entry1.getKey())
            && entry1.getValue().equals(new PrimitiveValue(entry2.getValue()))) {
          return false;
        }
      }
      return true;
    }
    return super.equals(o);
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

  public @NonNull ObjectValue set(@NonNull FieldPath path, @NonNull FieldValue value) {
    hardAssert(!path.isEmpty(), "Cannot set field for empty path on ObjectValue");
    String childName = path.getFirstSegment();
    if (path.length() == 1) {
      return setChild(childName, value);
    } else {
      FieldValue child = get(FieldPath.fromSingleSegment(childName));
      if (!(child instanceof ObjectValue)) {
        child = new ObjectValue();
      }
      child = ((ObjectValue) child).set(path.popFirst(), value);
      return setChild(childName, child);
    }
  }

  public @NonNull ObjectValue delete(@NonNull FieldPath path) {
    hardAssert(!path.isEmpty(), "Cannot delete field for empty path on ObjectValue");
    String childName = path.getFirstSegment();
    if (path.length() == 1) {
      return new ObjectValue(internalValue, overlays.insert(childName, null));
    } else {
      FieldValue child = get(FieldPath.fromSingleSegment(childName));
      if (child instanceof ObjectValue) {
        ObjectValue newChild = ((ObjectValue) child).delete(path.popFirst());
        return setChild(childName, newChild);
      } else {
        // Don't actually change a primitive value to an object for a delete.
        return this;
      }
    }
  }

  public @Nullable FieldValue get(@NonNull FieldPath path) {
    if (path.isEmpty()) {
      return this;
    }

    String childName = path.getFirstSegment();

    if (overlays.containsKey(childName)) {
      FieldValue fieldValue = overlays.get(childName);
      if (path.length() == 1) {
        return fieldValue;
      } else if (fieldValue instanceof ObjectValue) {
        return ((ObjectValue) fieldValue).get(path.popFirst());
      } else {
        return null;
      }
    } else {
      @Nullable Value value = this.internalValue.getMapValue().getFieldsMap().get(childName);
      int i;
      for (i = 1; isMap(value) && i < path.length(); ++i) {
        value = value.getMapValue().getFieldsMap().get(path.getSegment(i));
      }
      return value != null && i == path.length() ? new PrimitiveValue(value) : null;
    }
  }

  /** Recursively extracts the FieldPaths that are set in this ObjectValue. */
  @Override
  public @NonNull FieldMask getFieldMask() {
    Set<FieldPath> fields = new HashSet<>();
    for (Map.Entry<String, FieldValue> entry : this) {
      FieldPath currentPath = FieldPath.fromSingleSegment(entry.getKey());
      FieldValue value = entry.getValue();
      if (value instanceof ObjectValue) {
        FieldMask nestedMask = ((ObjectValue) value).getFieldMask();
        Set<FieldPath> nestedFields = nestedMask.getMask();
        if (nestedFields.isEmpty()) {
          // Preserve the empty map by adding it to the FieldMask.
          fields.add(currentPath);
        } else {
          // For nested and non-empty ObjectValues, add the FieldPath of the leaf nodes.
          for (FieldPath nestedPath : nestedFields) {
            fields.add(currentPath.append(nestedPath));
          }
        }
      } else {
        if (value.typeOrder() == TYPE_ORDER_OBJECT) {
          // make  a map value instead of a getFieldMask
          FieldMask nestedMask = ((PrimitiveValue) value).getFieldMask();
          Set<FieldPath> nestedFields = nestedMask.getMask();
          if (nestedFields.isEmpty()) {
            // Preserve the empty map by adding it to the FieldMask.
            fields.add(currentPath);
          } else {
            // For nested and non-empty ObjectValues, add the FieldPath of the leaf nodes.
            for (FieldPath nestedPath : nestedFields) {
              fields.add(currentPath.append(nestedPath));
            }
          }
        } else {
          fields.add(currentPath);
        }
      }
    }
    return FieldMask.fromSet(fields);
  }

  private ObjectValue setChild(String childName, FieldValue value) {
    return new ObjectValue(this.internalValue, overlays.insert(childName, value));
  }

  @NonNull
  @Override
  public Iterator<Map.Entry<String, FieldValue>> iterator() {
    return new Iterator<Map.Entry<String, FieldValue>>() {
      Iterator<Map.Entry<String, Value>> valueIterator = new SortedMapValueIterator(internalValue);
      @Nullable Map.Entry<String, Value> valuePeek = null;
      @Nullable Iterator<Map.Entry<String, FieldValue>> overlayIterator = overlays.iterator();
      Map.Entry<String, FieldValue> overlayPeek = null;

      private void peek() {
        if (valuePeek == null && valueIterator.hasNext()) {
          valuePeek = valueIterator.next();
        }

        if (overlayPeek == null && overlayIterator.hasNext()) {
          overlayPeek = overlayIterator.next();
        }

        if (valuePeek != null && overlayPeek != null && overlayPeek.getValue() == null) {
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
          Map.Entry<String, Value> result = valuePeek;
          valuePeek = null;
          return createMapEntry(result.getKey(), result.getValue());
        } else if (overlayPeek != null) {
          Map.Entry<String, FieldValue> result = overlayPeek;
          overlayPeek = null;
          return result;
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