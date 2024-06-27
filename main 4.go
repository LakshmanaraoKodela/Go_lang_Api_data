package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func main() {
	// jsonData, err := ioutil.ReadFile("sample2.json")
	// checkErr(err)
	// calling function to get dataframe from json data

	df := getJsonDf(getRespBytes("https://dummyjson.com/carts"))
	fmt.Print("\n", df)
	toCSV(df, "test_carts_1")
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
func getRespBytes(URL string) []byte {
	Response, err := http.Get(URL)
	checkErr(err)
	body, err := ioutil.ReadAll((Response.Body))
	checkErr(err)
	return body
}

func getJsonDf(jsondata []byte) dataframe.DataFrame {
	// unmarshal Json data into a Interface type
	var main_df dataframe.DataFrame
	var data interface{}
	err := json.Unmarshal(jsondata, &data)
	checkErr(err)

	// check if data is MapstringInterface type
	if obj, ok := data.([]interface{}); ok {
		// fmt.Print(obj)
		var df dataframe.DataFrame
		var jsonstr []string
		for _, v := range obj {
			if data, ok := v.(map[string]interface{}); ok {
				byt, err := json.Marshal(data)
				checkErr(err)
				df_fun := getJsonDf(byt)
				df = df.Concat(df_fun)
			} else {
				// fmt.Print("\nline 55----\n", v)
				str := fmt.Sprintf("%v", v)
				jsonstr = append(jsonstr, str)
			}
		}
		if len(jsonstr) > 0 {
			df = dataframe.New(
				series.New(jsonstr, series.String, " "),
			)
		}
		return df
	} else if obj, ok := data.(map[string]interface{}); ok {
		// fmt.Print(obj)
		var df_Json, df_map dataframe.DataFrame
		var keys []string
		var nestkey string
		for k, v := range obj {
			var df_keys dataframe.DataFrame //x
			if obj, ok := v.(map[string]interface{}); ok {
				// ----------------------------------------------------------------
				byt, err := json.Marshal(obj)
				checkErr(err)
				nestkey = k
				df_keys = getJsonDf(byt)
				// need to update about lists here like {hair:{color:white,type:strong}}|
			} else if val, ok := v.([]interface{}); !ok {
				formatkey := fmt.Sprintf("\"%v\":\"%v\"", k, v)
				keys = append(keys, formatkey)
			} else {
				// fmt.Print("----its a map -----------\n")
				nestkey = k
				byt, err := json.Marshal(val)
				checkErr(err)
				df_Json = getJsonDf(byt)
			}
			if df_keys.Nrow() > 0 {
				df_keys = renameColumn_WithNestedKey(df_keys, nestkey)
				if df_map.Ncol() == 0 {
					df_map = df_map.Concat(df_keys)
				} else {
					df_map = df_keys.CrossJoin(df_map)
				}
			}
		}
		jsonstr := "[{" + strings.Join(keys, ",") + "}]"
		df_outjson := dataframe.ReadJSON(strings.NewReader(jsonstr))
		if df_map.Nrow() > 0 {
			df_outjson = df_outjson.CrossJoin(df_map)
		}
		if df_Json.Nrow() > 0 {
			// renaming the nested columns with regarding to the parent key
			df_Json = renameColumn_WithNestedKey(df_Json, nestkey)
			df_outjson = df_Json.CrossJoin(df_outjson)
		}
		return df_outjson
	}
	return main_df
}

func toCSV(df dataframe.DataFrame, filename string) {
	file, err := os.Create("../dummyJSON/try1/" + filename + ".csv")
	checkErr(err)
	defer file.Close()
	df.WriteCSV(file)
	fmt.Printf("%v.csv created Successfully.✅✅✅", filename)
}
func renameColumn_WithNestedKey(df dataframe.DataFrame, key string) dataframe.DataFrame {
	for _, k := range df.Names() {
		if k == " " {
			df = df.Rename(key, k)
		} else {
			df = df.Rename(key+"_"+k, k)
		}
	}
	return df
}
