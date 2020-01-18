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

package com.google.firebase.firestore.model.value;

import androidx.annotation.Nullable;
import com.google.firebase.firestore.model.FieldPath;
import com.google.firebase.firestore.model.mutation.FieldMask;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.ValueOrBuilder;
import java.util.HashMap;
import java.util.Map;

public class ProtobufValue extends FieldValue {
  final Value fieldsProto;

  public ProtobufValue(Map<String, Value> fieldsProto) {
    this.fieldsProto =
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(fieldsProto).build())
            .build();
    ;
  }

  public ProtobufValue(Value fieldsProto) {
    this.fieldsProto = fieldsProto;
  }

  public static ProtobufValue emptyObject() {
    return new ProtobufValue(new HashMap<>());
  }

  /**
   * Returns the value at the given path or null.
   *
   * @param fieldPath the path to search
   * @return The value at the path or if there it doesn't exist.
   */
  public @Nullable ValueOrBuilder get(FieldPath fieldPath) {
    return extractValue(fieldsProto, fieldPath);
  }

  public Value value() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    return false;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public FieldMask getFieldMask() {
    return null;
  }

  static ValueOrBuilder extractValue(ValueOrBuilder fieldsProto, FieldPath fieldPath) {
    ValueOrBuilder value = fieldsProto;

    for (int i = 0;
        value != null
            && value.getValueTypeCase() == Value.ValueTypeCase.MAP_VALUE
            && i < fieldPath.length();
        ++i) {
      value = value.getMapValue().getFieldsMap().get(fieldPath.getSegment(i));
    }

    return value;
  }
}
