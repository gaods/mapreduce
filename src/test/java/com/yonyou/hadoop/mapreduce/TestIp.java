package com.yonyou.hadoop.mapreduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

public class TestIp {

	static String filepath = "/Users/gaods/Documents/hadoop/data/dataip.csv";
	static java.text.DateFormat format2 = new java.text.SimpleDateFormat("yyyyMMddhhmmss");

	private static String getData() {
		String data = "";

		System.out.print("data start");
		for (int i = 1; i <= 1000000; i++) {
			data += "192.168.0." + i + " " + (format2.format(new Date(System.currentTimeMillis())))+"\n";
		}
		System.out.print("data end");

		return data;
	}

	public static void main(String[] args) {
		FileOutputStream fop = null;
		File file;
	

		try {
			
			String content = getData();

			file = new File(filepath);
			fop = new FileOutputStream(file);

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			// get the content in bytes
			byte[] contentInBytes = content.getBytes();

			fop.write(contentInBytes);
			fop.flush();
			fop.close();

			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fop != null) {
					fop.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}


}
