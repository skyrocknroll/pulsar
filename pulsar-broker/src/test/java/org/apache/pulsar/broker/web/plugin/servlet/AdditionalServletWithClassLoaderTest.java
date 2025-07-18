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
package org.apache.pulsar.broker.web.plugin.servlet;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.eclipse.jetty.servlet.ServletHolder;
import org.testng.annotations.Test;


/**
 * Unit test {@link AdditionalServletWithClassLoader}.
 */
@Test(groups = "broker")
public class AdditionalServletWithClassLoaderTest {

    @Test
    public void testWrapper() {
        AdditionalServlet servlet = mock(AdditionalServlet.class);
        NarClassLoader loader = mock(NarClassLoader.class);
        AdditionalServletWithClassLoader wrapper = new AdditionalServletWithClassLoader(servlet, loader);
        // test getBasePath
        String basePath = "bathPath";
        when(servlet.getBasePath()).thenReturn(basePath);
        assertEquals(basePath, wrapper.getBasePath());
        verify(servlet, times(1)).getBasePath();
        // test loadConfig
        ServiceConfiguration conf = new ServiceConfiguration();
        wrapper.loadConfig(conf);
        verify(servlet, times(1)).loadConfig(same(conf));
        // test getServlet
        assertEquals(wrapper.getServlet(), servlet);
        // test getServletHolder
        ServletHolder servletHolder = new ServletHolder();
        when(servlet.getServletHolder()).thenReturn(servletHolder);
        assertEquals(wrapper.getServletHolder(), servletHolder);
        verify(servlet, times(1)).getServletHolder();
    }

    @Test
    public void testClassLoaderSwitcher() throws Exception {
        NarClassLoader narLoader = mock(NarClassLoader.class);
        AdditionalServlet servlet = new AdditionalServlet() {
            @Override
            public void loadConfig(PulsarConfiguration pulsarConfiguration) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }

            @Override
            public String getBasePath() {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
                return "base-path";
            }

            @Override
            public ServletHolder getServletHolder() {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
                return null;
            }

            @Override
            public void close() {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
        };

        AdditionalServletWithClassLoader additionalServletWithClassLoader =
                new AdditionalServletWithClassLoader(servlet, narLoader);
        ClassLoader curClassLoader = Thread.currentThread().getContextClassLoader();
        // test class loader
        assertEquals(additionalServletWithClassLoader.getClassLoader(), narLoader);
        // test getBasePath
        assertEquals(additionalServletWithClassLoader.getBasePath(), "base-path");
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test loadConfig
        ServiceConfiguration conf = new ServiceConfiguration();
        additionalServletWithClassLoader.loadConfig(conf);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test getServletHolder
        assertNull(additionalServletWithClassLoader.getServletHolder());
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test getServlet
        assertEquals(additionalServletWithClassLoader.getServlet(), servlet);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test close
        additionalServletWithClassLoader.close();
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);

    }
}
