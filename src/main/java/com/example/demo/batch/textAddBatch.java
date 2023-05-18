package com.example.demo.batch;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

public class textAddBatch {

	public static void main(String[] args) {
		try {
			// データベースに接続するための情報を設定する
			String url = "jdbc:postgresql://localhost:5432/data_processing";
			String user = "postgres";
			String password = "postgres";
			Connection conn = DriverManager.getConnection(url, user, password);
			
			// CSVファイルを読み込む(いらない情報も消去)
			List<String[]> prefectureData = readData(
					"https://techtech-sorae.com/wp-content/uploads/2021/07/pref_lat_lon.csv");
			//ダウンロード先：https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_view_cities_wards.cgi
			List<String[]> municipalitiesData = readData("C:\\Users\\yuuta_000\\Downloads\\市区町村緯度経度_sjis.csv");
			
			List<String[]> newMunicipalitiesData = deleteMunicipalitiesData(municipalitiesData);
			municipalitiesData = null;
			List<String[]> addressData = readData("https://geolonia.github.io/japanese-addresses/latest.csv");
			List<String[]> newAddressData = deleteAddressData(addressData);
			List<String[]> institutionData = readDataAsShiftJis(
					"https://www.opendata.metro.tokyo.lg.jp/suisyoudataset/130001_public_facility.csv");
			//ダウンロード先:https://github.com/code4fukui/BaseRegistry/blob/main/%E8%A1%8C%E6%94%BF%E5%9F%BA%E6%9C%AC%E6%83%85%E5%A0%B1%E3%83%87%E3%83%BC%E3%82%BF%E9%80%A3%E6%90%BA%E3%83%A2%E3%83%87%E3%83%AB-POI%E3%82%B3%E3%83%BC%E3%83%89.md
			List<String[]> poiData = readData("C:\\Users\\yuuta_000\\Downloads\\POIコード.csv");
			//用量削減
			addressData = null;

			// データをデータベースに挿入する
			//場所データ
			insertPlaceData(prefectureData, newMunicipalitiesData, newAddressData, conn);
			//施設データ
			insertInstitutionsData(institutionData, poiData, conn);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	//データ読み込み(ダウンロードするCSVの読み込み型により条件分岐)
	public static List<String[]> readData(String ex) throws IOException {
		BufferedReader reader;
		List<String[]> result = new ArrayList<>();
		if (ex.contains("http")) {
			reader = new BufferedReader(new InputStreamReader(new URL(ex).openStream(), StandardCharsets.UTF_8));
		} else if (ex.contains("sjis")) {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(ex), "Shift-JIS"));
		} else {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(ex), StandardCharsets.UTF_8));
		}
		;
		String line;
		while ((line = reader.readLine()) != null) {
			String[] fields = line.split(",");
			result.add(fields);
		}
		reader.close();
		return result;
	}

	//該当ファイルのみ、表示の型と改行等が入っているため別処理を作成
	public static List<String[]> readDataAsShiftJis(String ex) throws IOException {
	    List<String[]> result = new ArrayList<>();
	    BufferedReader reader = new BufferedReader(
	            new InputStreamReader(new URL(ex).openStream(), Charset.forName("Shift-JIS")));
	    String line;
	    String beforeLine = "";
	    boolean firstLine = true;
	    while ((line = reader.readLine()) != null) {
	        if (firstLine) {
	            firstLine = false;
	            continue;
	        }
	        //２列にわたって記載されているデータを結合
	        if (!line.endsWith("28")) {
	            beforeLine += line;
	            continue;
	        }
	        line = beforeLine + line;
	        String[] fields = line.split(",");
	        result.add(fields);
	        beforeLine = "";
	    }
	    reader.close();
	    return result;
	}

	//DBに場所データをinsertする
	public static void insertPlaceData(List<String[]> prefactureData, List<String[]> municipalitiesData,
			List<String[]> addressData, Connection conn) throws SQLException {
		boolean firstIteration = true;
		createTabel(conn);
		PreparedStatement prepstmt = conn
				.prepareStatement("INSERT INTO prefectures (name,longitude,latitude) VALUES (?,?,?) ;");
		for (String[] fields : prefactureData) {
			if (firstIteration) {
				firstIteration = false;
				continue;
			}
			prepstmt.setString(1, fields[0]);
			prepstmt.setBigDecimal(2, new BigDecimal(fields[1]));
			prepstmt.setBigDecimal(3, new BigDecimal(fields[2]));
			prepstmt.executeUpdate();
		}

		PreparedStatement municipalpstmt = conn
				.prepareStatement("INSERT INTO municipalities (name,longitude,latitude,parentNum) VALUES (?,?,?,?) ;");
		Map<String, Integer> map = new HashMap<String, Integer>();
		int count = 0;
		for (String[] fields : municipalitiesData) {
			count++;
			if (!fields[5].equals("NA")) {
				fields[4] = fields[4] + fields[5];
			}
			municipalpstmt.setString(1, fields[4]);
			municipalpstmt.setBigDecimal(2, new BigDecimal(fields[6]));
			municipalpstmt.setBigDecimal(3, new BigDecimal(fields[7]));
			municipalpstmt.setInt(4, 13);
			municipalpstmt.executeUpdate();
			map.put(fields[4], count);
		}
		PreparedStatement addresspstmt = conn
				.prepareStatement("INSERT INTO address (name,longitude,latitude,parentNum) VALUES (?,?,?,?) ;");
		for (String[] fields : addressData) {
			addresspstmt.setString(1, fields[8].replaceAll("\"", ""));

			if (fields.length >= 12) {
				addresspstmt.setBigDecimal(2, new BigDecimal(fields[12]));
				addresspstmt.setBigDecimal(3, new BigDecimal(fields[13]));
			}
			String key = fields[5].replaceAll("\"", "");
			addresspstmt.setInt(4, map.get(key));
			addresspstmt.executeUpdate();
		}

	}

	//DBに施設データをinsertする
	public static void insertInstitutionsData(List<String[]> institutiosData, List<String[]> poiData, Connection conn)
			throws SQLException {
		Map<String, Integer> map = new HashMap<String, Integer>();
		boolean firstIteration = true;
		PreparedStatement institutionpstmt = conn.prepareStatement(
				"INSERT INTO institution (name,longitude,latitude,available_days,start_time,end_time,phone_number,address,postal_code,tag) VALUES (?,?,?,?,?,?,?,?,?,?) ;");
		PreparedStatement tagpstmt = conn.prepareStatement("INSERT INTO  tag_species (name) VALUES (?) ;");
		int num = 1;
		for (String[] fields : poiData) {
			if (firstIteration) {
				firstIteration = false;
				continue;
			}
			tagpstmt.setString(1, fields[2]);
			map.put(fields[1], num);
			num++;
			tagpstmt.execute();
		}

		for (String[] fields : institutiosData) {
			System.out.println(fields[5]+":"+fields[19]);
			institutionpstmt.setString(1, fields[4]);
			institutionpstmt.setBigDecimal(2, new BigDecimal(fields[10]));
			institutionpstmt.setBigDecimal(3, new BigDecimal(fields[11]));
			institutionpstmt.setString(4, fields[16]);
			institutionpstmt.setString(5, fields[17]);
			institutionpstmt.setString(6, fields[18]);
			institutionpstmt.setString(7, fields[12]);
			institutionpstmt.setString(8, fields[8]);
			institutionpstmt.setString(9, fields[24]);
			institutionpstmt.setInt(10, map.get(fields[7].replaceAll("a", "")));
			institutionpstmt.execute();
		}
	}

	//テーブルを作成するメソッド（既にある場合は一度消去）
	public static void createTabel(Connection conn) throws SQLException {
		String[] tableNames = { "prefectures", "municipalities", "address" };
		for (int i = 0; i < tableNames.length; i++) {
			StringBuilder deletesql = new StringBuilder();
			deletesql.append("DROP TABLE IF EXISTS ");
			deletesql.append(tableNames[i]);
			deletesql.append(";");
			StringBuilder sql = new StringBuilder();
			sql.append("CREATE TABLE ");
			sql.append(tableNames[i]);
			sql.append(" (id SERIAL PRIMARY KEY, name VARCHAR(255),longitude DECIMAL(9, 6), latitude DECIMAL(9, 6)");
			if (i == 1 || i == 2) {
				sql.append(", parentNum Integer ");
			}
			;
			sql.append(");");
			PreparedStatement deletePstmt = conn.prepareStatement(deletesql.toString());
			deletePstmt.execute();
			PreparedStatement createPstmt = conn.prepareStatement(sql.toString());
			createPstmt.execute();
		}
		String deleteInstitutionSql = "DROP TABLE IF EXISTS institution;";
		String deleteTagSql = "DROP TABLE IF EXISTS tag_species;";
		PreparedStatement deletePstmt = conn.prepareStatement(deleteInstitutionSql);
		deletePstmt.execute();
		deletePstmt = conn.prepareStatement(deleteTagSql);
		deletePstmt.execute();
		String createInstitutionSql = "CREATE TABLE institution (id SERIAL PRIMARY KEY, name VARCHAR(255),longitude DECIMAL(9, 6), latitude DECIMAL(9, 6),available_days VARCHAR(255),start_time VARCHAR(255),end_time VARCHAR(255),phone_number VARCHAR(20),address VARCHAR(255),postal_code VARCHAR(10),tag VARCHAR(255));";
		PreparedStatement institutionPstmt = conn.prepareStatement(createInstitutionSql);
		institutionPstmt.execute();
		String tagSql = "CREATE TABLE tag_species (id SERIAL PRIMARY KEY ,name VARCHAR(255));";
		PreparedStatement tagPstmt = conn.prepareStatement(tagSql);
		tagPstmt.execute();
	}

	//東京以外のデータを処理するメソッド
	public static List<String[]> deleteMunicipalitiesData(List<String[]> datas) {
		List<String[]> newData = new ArrayList<String[]>();
		for (String[] data : datas) {
			if (data[3].contains("東京")) {
				newData.add(data);
			}
		}
		return newData;
	}

	//東京以外のデータを消去するメソッド
	public static List<String[]> deleteAddressData(List<String[]> datas) {
		List<String[]> newData = new ArrayList<String[]>();
		for (String[] data : datas) {
			if (data[1].toString().contains("東京")) {
				newData.add(data);

			}
		}
		return newData;
	}

}
