package fi.helsinki.cs.bsmr.master;

import static org.junit.Assert.*;

import fi.helsinki.cs.bsmr.fs.Util;

import org.junit.Test;

public class UtilTest
{
	@Test
	public void testPathFix() {
		String input = "/static/data/loremipsum.txt";
		String output = Util.fixPathSeparators(input);
		assertEquals(input, output);
	}
}
