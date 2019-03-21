/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.file.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.splitter.FileSplitter;
import org.springframework.integration.json.JsonPathUtils;
import org.springframework.integration.support.json.JsonObjectMapperProvider;
import org.springframework.integration.test.matcher.HeaderMatcher;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.MimeTypeUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Gary Russell
 * @author Artem Bilan
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
public abstract class FileSourceTests {

	private static final String TMPDIR = System.getProperty("java.io.tmpdir");

	private static final String ROOT_DIR = TMPDIR + File.separator + "dataflow-tests"
			+ File.separator + "input";

	@Autowired
	protected Source source;

	@Autowired
	protected MessageCollector messageCollector;

	protected ObjectMapper objectMapper = new ObjectMapper();

	protected File atomicFileCreate(String filename) throws IOException {
		File file = new File(ROOT_DIR, filename + ".tmp");
		File fileFinal = new File(ROOT_DIR, filename);
		file.delete();
		file.deleteOnExit();
		fileFinal.delete();
		fileFinal.deleteOnExit();
		FileOutputStream fos = new FileOutputStream(file);
		fos.write("this is a test\nline2\n".getBytes());
		fos.close();
		assertTrue(file.renameTo(fileFinal));
		return fileFinal;
	}

	@TestPropertySource(properties = {
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests${file.separator}input",
			"trigger.fixedDelay = 100",
			"trigger.timeUnit = MILLISECONDS" })
	public static class ContentPayloadTests extends FileSourceTests {

		@Test
		public void testSimpleFile() throws Exception {
			String filename = "test.txt";
			File file = atomicFileCreate(filename);
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(byte[].class));
			assertEquals("this is a test\nline2\n", new String((byte[]) received.getPayload()));
			assertThat(received, HeaderMatcher.hasHeader(FileHeaders.FILENAME, filename));
			assertThat(received, HeaderMatcher.hasHeader(FileHeaders.RELATIVE_PATH, filename));
			assertThat(received, HeaderMatcher.hasHeader(FileHeaders.ORIGINAL_FILE, file));
			file.delete();
		}

	}

	@TestPropertySource(properties = {
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests${file.separator}input",
			"trigger.fixedDelay = 100",
			"trigger.timeUnit = MILLISECONDS",
			"file.consumer.mode = ref",
			"spring.cloud.stream.bindings.output.contentType=text/plain" })
	public static class FilePayloadRefModeTextPlainContentTypeTests extends FileSourceTests {

		@Test
		public void testSimpleFile() throws Exception {
			File file = atomicFileCreate("test.txt");
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			// since we've explicitly asked for text/plain (above property ...output.contentType), payload should
			// come back as is, no need to read it as a File
			assertEquals(file.getAbsolutePath(), received.getPayload());
			file.delete();
		}
	}

	@TestPropertySource(properties = {
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests${file.separator}input",
			"trigger.fixedDelay = 100",
			"trigger.timeUnit = MILLISECONDS",
			"file.consumer.mode = ref",
			"spring.cloud.stream.bindings.output.contentType=application/json" })
	public static class FilePayloadRefModeApplicationJsonContentTypeTests extends FileSourceTests {

		@Test
		public void testSimpleFile() throws Exception {
			File file = atomicFileCreate("test.txt");
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertEquals(file.getAbsolutePath(), objectMapper.readValue((String) received.getPayload(), File.class).getAbsolutePath());
			file.delete();
		}
	}


	@TestPropertySource(properties = {
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests${file.separator}input",
			"trigger.fixedDelay = 100",
			"trigger.timeUnit = MILLISECONDS",
			"file.consumer.mode = ref"})
	public static class FilePayloadRefModeDefaultContentTypeTests extends FileSourceTests {

		@Test
		public void testSimpleFile() throws Exception {
			File file = atomicFileCreate("test.txt");
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertEquals(file.getAbsolutePath(), objectMapper.readValue((String) received.getPayload(), File.class).getAbsolutePath());
			assertEquals(MimeTypeUtils.APPLICATION_JSON, received.getHeaders().get(MessageHeaders.CONTENT_TYPE));
			file.delete();
		}

	}

	@TestPropertySource(properties = {
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests${file.separator}input",
			"trigger.fixedDelay = 100",
			"trigger.timeUnit = MILLISECONDS",
			"file.consumer.mode = lines" })
	public static class LinesPayloadTests extends FileSourceTests {

		@Test
		public void testSimpleFile() throws Exception {
			File file = atomicFileCreate("test.txt");
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));
			assertEquals("this is a test", received.getPayload());
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));
			assertEquals("line2", received.getPayload());
			file.delete();
		}

	}

	@TestPropertySource(properties = {
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests${file.separator}input",
			"trigger.fixedDelay = 100",
			"trigger.timeUnit = MILLISECONDS",
			"file.consumer.mode = lines",
			"file.consumer.withMarkers = true" })
	public static class LinesAndMarkersAsJsonPayloadTests extends FileSourceTests {

		@Test
		public void testSimpleFile() throws Exception {
			File file = atomicFileCreate("test.txt");
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));
			assertEquals(FileSplitter.FileMarker.Mark.START.name(),
					JsonPathUtils.evaluate(received.getPayload(), "$.mark"));
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));
			assertEquals("this is a test", received.getPayload());
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));
			assertEquals("line2", received.getPayload());
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			Object fileMarker = received.getPayload();
			assertThat(fileMarker, Matchers.instanceOf(String.class));
			assertEquals(FileSplitter.FileMarker.Mark.END.name(), JsonPathUtils.evaluate(fileMarker, "$.mark"));
			FileSplitter.FileMarker fileMarker1 = JsonObjectMapperProvider.newInstance()
					.fromJson(fileMarker, FileSplitter.FileMarker.class);
			assertEquals(FileSplitter.FileMarker.Mark.END, fileMarker1.getMark());
			assertEquals(file.getAbsolutePath(), fileMarker1.getFilePath());
			assertEquals(2, fileMarker1.getLineCount());
			file.delete();
		}

	}

	@TestPropertySource(properties = {
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests${file.separator}input",
			"trigger.fixedDelay = 100",
			"trigger.timeUnit = MILLISECONDS",
			"file.consumer.mode = ref",
			"file.filenamePattern = *.txt" })
	public static class FilePayloadWithPatternTests extends FileSourceTests {

		@Test
		public void testSimpleFile() throws Exception {
			File file = atomicFileCreate("test.txt");
			File hidden = atomicFileCreate("test.foo");
			assertTrue(new File(ROOT_DIR, "test.foo").exists());
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertEquals(file, objectMapper.readValue((String) received.getPayload(), File.class));
			received = messageCollector.forChannel(source.output()).poll(300, TimeUnit.MILLISECONDS);
			assertNull(received);
			file.delete();
			hidden.delete();
		}

	}

	@TestPropertySource(properties = {
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests${file.separator}input",
			"trigger.fixedDelay = 100",
			"trigger.timeUnit = MILLISECONDS",
			"file.consumer.mode = ref",
			"file.filenameRegex = .*.txt" })
	public static class FilePayloadWithRegexTests extends FileSourceTests {

		@Test
		public void testSimpleFile() throws Exception {
			File file = atomicFileCreate("test.txt");
			File hidden = atomicFileCreate("test.foo");
			assertTrue(new File(ROOT_DIR, "test.foo").exists());
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertEquals(file, objectMapper.readValue((String) received.getPayload(), File.class));
			received = messageCollector.forChannel(source.output()).poll(300, TimeUnit.MILLISECONDS);
			assertNull(received);
			file.delete();
			hidden.delete();
		}

	}

	@SpringBootApplication
	static class FileSourceApplication {

		public static void main(String[] args) {
			SpringApplication.run(FileSourceApplication.class, args);
		}

	}


}
