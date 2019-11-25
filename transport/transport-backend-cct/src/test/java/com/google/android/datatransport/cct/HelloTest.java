// Copyright 2019 Google LLC
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

package com.google.android.datatransport.cct;

import com.google.android.datatransport.cct.proto.AndroidClientInfo;
import com.google.android.datatransport.cct.proto.BatchedLogRequest;
import com.google.android.datatransport.cct.proto.ClientInfo;
import com.google.android.datatransport.cct.proto.LogEvent;
import com.google.android.datatransport.cct.proto.LogRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;

@RunWith(JUnit4.class)
public class HelloTest {
  @Test
  public void test() throws InvalidProtocolBufferException {
    BatchedLogRequest request =
        BatchedLogRequest.newBuilder()
            .addLogRequest(
                LogRequest.newBuilder()
                    .setClientInfo(
                        ClientInfo.newBuilder()
                            .setClientType(ClientInfo.ClientType.ANDROID)
                            .setAndroidClientInfo(
                                AndroidClientInfo.newBuilder()
                                    .setCountry("CA")
                                    .setBrand("Hello")
                                    .build())
                            .build())
                    .setLogSourceName("HELLO")
                    .addLogEvent(
                        LogEvent.newBuilder()
                            .setSourceExtension(ByteString.copyFromUtf8("hello"))
                            .build())
                    .addLogEvent(
                        LogEvent.newBuilder().setSourceExtensionJsonProto3("\\{\\}").build())
                    .build())
            .build();

    String json = JsonFormat.printer().omittingInsignificantWhitespace().print(request);

    BatchedLogRequest.Builder parsed = BatchedLogRequest.newBuilder();
    JsonFormat.parser().merge(json, parsed);

    assertThat(parsed.build()).isEqualTo(request);
  }
}
