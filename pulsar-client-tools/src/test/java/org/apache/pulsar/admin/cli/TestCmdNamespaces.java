/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.admin.cli;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestCmdNamespaces {

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws IOException {
        //NOTHING FOR NOW
    }


    @Test
    public void testSetRetentionCmd() throws Exception {
        Namespaces namespaces = mock(Namespaces.class);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.namespaces()).thenReturn(namespaces);

        CmdNamespaces cmd = new CmdNamespaces(() -> admin);

        cmd.run("set-retention public/default -s 2T -t 200d".split("\\s+"));
        verify(namespaces, times(1)).setRetention("public/default",
                new RetentionPolicies(200 * 24 * 60, 2 * 1024 * 1024));
   }
}
