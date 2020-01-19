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

package com.google.firebase.firestore.util;

import com.google.firestore.v1.Value;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

public class SortedMapValueIterator implements Iterator<Map.Entry<String, Value>> {

  private final Map<String, Value> mapValue;
  private final Iterator<String> iterator;

  public SortedMapValueIterator(Value value) {
    // assert is map
    this.mapValue = value.getMapValue().getFieldsMap();
    this.iterator = new TreeSet<>(mapValue.keySet()).iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Map.Entry<String, Value> next() {
    String key = iterator.next();
    Value value = mapValue.get(key);

    return new Map.Entry<String, Value>() {
      @Override
      public String getKey() {
        return key;
      }

      @Override
      public Value getValue() {
        return value;
      }

      @Override
      public Value setValue(Value value) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
