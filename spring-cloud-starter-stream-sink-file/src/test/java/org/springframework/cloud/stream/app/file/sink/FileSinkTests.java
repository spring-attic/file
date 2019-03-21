/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.app.file.sink;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileCopyUtils;

/**
 * @author Mark Fisher
 * @author Artem Bilan
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
public abstract class FileSinkTests {

	private static final String TMPDIR = System.getProperty("java.io.tmpdir");

	private static final String ROOT_DIR = TMPDIR + File.separator + "dataflow-tests";

	@Autowired
	protected Sink sink;

	@TestPropertySource(properties = {
			"file.name = test",
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests",
			"file.suffix=txt" })
	public static class TextTests extends FileSinkTests {

		@Test
		public void test() throws Exception {
			sink.input().send(MessageBuilder.withPayload("this is a test").build());
			File file = new File(ROOT_DIR, "test.txt");
			file.deleteOnExit();
			assertTrue("file does not exist", file.exists());
			assertEquals("this is a test" + System.lineSeparator(), FileCopyUtils.copyToString(new FileReader(file)));
		}

	}

	@TestPropertySource(properties = {
			"file.binary = true",
			"file.directory = ${java.io.tmpdir}${file.separator}dataflow-tests" })
	public static class BinaryTests extends FileSinkTests {

		@Test
		public void test() throws Exception {
			sink.input().send(MessageBuilder.withPayload("foo".getBytes()).build());
			File file = new File(ROOT_DIR, "file-sink");
			file.deleteOnExit();
			assertTrue("file does not exist", file.exists());
			byte[] results = FileCopyUtils.copyToByteArray(file);
			assertEquals(3, results.length);
			assertArrayEquals("foo".getBytes(), results);
		}

	}

	@TestPropertySource(properties = {
			"file.nameExpression = payload.substring(0, 4)",
			"file.directoryExpression = '${java.io.tmpdir}${file.separator}dataflow-tests${file.separator}'+headers.dir",
			"file.suffix=out" })
	public static class ExpressionTests extends FileSinkTests {

		@Test
		public void test() throws Exception {
			sink.input().send(MessageBuilder.withPayload("this is another test")
					.setHeader("dir", "expression").build());
			File file = new File(ROOT_DIR + File.separator + "expression", "this.out");
			file.deleteOnExit();
			assertTrue("file does not exist", file.exists());
			assertEquals("this is another test" + System.lineSeparator(),
					FileCopyUtils.copyToString(new FileReader(file)));
		}

	}

	@SpringBootApplication
	static class FileSinkApplication {

		public static void main(String[] args) {
			SpringApplication.run(FileSinkApplication.class, args);
		}

	}

}
