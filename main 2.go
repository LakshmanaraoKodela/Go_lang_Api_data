package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/monodeep12/flattenjson"
)

func main() {
	// calling the function for reading the user input for URL
	Url := ReadInput("URL")
	// calling the function for getting the API response bytes
	respBytes := getRespBytes(Url)

	//  identify the json type based on the response data of API.
	switch determainJSON(respBytes) {
	case "Array of object":
		NormalJson(Url, respBytes)
	case "Object":
		NestedJson(string(respBytes))
	default:
		fmt.Print("unkown")
	}

}

// function for parsing normal json
func NormalJson(URL string, respBytes []byte) {
	fmt.Println("Please Wait...\nProccessing your Url... \n\n\n")
	// getting data from the API in bytes format
	data := respBytes
	// get file name from the URL
	filename := Get_filename(URL)
	// transform API json data into Dataframe
	df := dataframe.ReadJSON(strings.NewReader(string(data)))
	// write dataframe to csv file
	toCSV(df, filename)
}

func NestedJson(jsonData string) {
	// Define an empty interface{} to hold the decoded JSON data
	var data interface{}

	// Unmarshal JSON data into the empty interface{}
	err := json.Unmarshal([]byte(jsonData), &data)
	if err != nil {
		fmt.Printf("Error unmarshaling JSON: %v\n", err)
		return
	}
	var df_json, df_outjson, final_df dataframe.DataFrame
	// Use type assertion to inspect the types of values
	if obj, ok := data.(map[string]interface{}); ok {
		var keys []string
		for key, value := range obj {
			// fmt.Print(value, "\n\n")

			if _, ok := value.([]interface{}); !ok {
				jsonstr := fmt.Sprintf("\"%v\":\"%v\"", key, value)
				// fmt.Print(jsonstr, "\n\n")
				keys = append(keys, jsonstr)
			} else if v, ok := value.([]interface{}); ok {
				// fmt.Printf("Key: %s, Type: %T\n\n", key, v)
				df_json = toDF(v)
			}
		}
		if len(keys) != 0 {
			jsonstr := "[{" + strings.Join(keys, ",") + "}]"
			fmt.Println(jsonstr)
			df_outjson = dataframe.ReadJSON(strings.NewReader(jsonstr))
			final_df = df_json.CrossJoin(df_outjson)
			toCSV(final_df, ReadInput("fileName")+"_withKeys")
		} else {
			toCSV(df_json, ReadInput("fileName")+"_noKeys")
		}
	}
}

// function for reading user input Api Url
func ReadInput(word string) string {
	// reading user input Url
	// creating a scanner
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("Enter the %v: ", word)
	scanner.Scan()
	url := scanner.Text()
	// fmt.Println("Given url is Url: ", url) //printing the URL for user confirmation
	return url
}

// function for getting the API response bytes
func getRespBytes(URL string) []byte {
	Response, err := http.Get(URL)
	checkNilErr(err)
	body, err := ioutil.ReadAll((Response.Body))
	checkNilErr(err)
	return body
}

// function for determain the json type
func determainJSON(JsonStr []byte) string {
	var data interface{}
	// unmarshalling data into empty interface
	err := json.Unmarshal(JsonStr, &data)
	checkNilErr(err)
	// checking if data is array or object
	switch data.(type) {
	case []interface{}:
		return "Array of object"
	case map[string]interface{}:
		return "Object"
	default:
		return "Unknown"
	}
}

// function for checking the Nil error
func checkNilErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Get_filename(URL string) string {
	parts := strings.Split(URL, "/")
	i := parts[len(parts)-1]
	return i
}

func toDF(jsonData []interface{}) dataframe.DataFrame {
	var dataStr []string //result Json String Data array.
	for i := range jsonData {
		formatdata, err := json.Marshal(jsonData[i])
		checkNilErr(err)
		//  flatting json
		record, err := flattenjson.JSONByte(formatdata, "_", false)
		checkNilErr(err)
		// fmt.Print(string(record), "\n\n")
		dataStr = append(dataStr, string(record))
	}
	data2 := "[" + strings.Join(dataStr, ",") + "]"
	df := dataframe.ReadJSON(strings.NewReader(data2))
	return df

}

func toCSV(df dataframe.DataFrame, filename string) {
	file, err := os.Create(filename + ".csv")
	checkNilErr(err)
	defer file.Close()
	df.WriteCSV(file)
	fmt.Printf("%v.csv created Successfully.✅✅✅", filename)
}

// func nested_Json(Url string, Apidata []byte) {
// 	var data interface{}
// 	err := json.Unmarshal(Apidata, &data)
// 	checkNilErr(err)

// }
