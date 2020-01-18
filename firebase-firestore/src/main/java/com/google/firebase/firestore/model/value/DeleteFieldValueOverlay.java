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
import com.google.firestore.v1.ValueOrBuilder;

public class DeleteFieldValueOverlay extends FieldValue {
  private FieldValue baseValue;
  private FieldPath path;

  public DeleteFieldValueOverlay(FieldValue baseValue, FieldPath path) {
    this.baseValue = baseValue;
    this.path = path;
  }

  @Nullable
  @Override
  public ValueOrBuilder get(FieldPath fieldPath) {
    return path.isPrefixOf(fieldPath) ? null : baseValue.get(fieldPath);
  }

  @Override
  public FieldMask getFieldMask() {
    return baseValue.getFieldMask(); // remove
  }
}