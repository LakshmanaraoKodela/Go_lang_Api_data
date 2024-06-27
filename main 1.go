package main

import (
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
	url := "https://dummyjson.com/users"
	// url := "https://api.nationalize.io/?name=nathaniel"
	// url := "https://jsonplaceholder.typicode.com/users"
	// url = "https://chroniclingamerica.loc.gov/newspapers.json"
	// url = "https://api.crossref.org/journals?query=pharmacy+health"
	data := string(getRespBytes(url))
	determainJSON(data)
	// fmt.Println(data)
	// check(data)

}

func getRespBytes(URL string) []byte {
	Response, err := http.Get(URL)
	checkNilErr(err)
	body, err := ioutil.ReadAll((Response.Body))
	checkNilErr(err)
	return body
}
func checkNilErr(err error) {
	if err != nil {
		panic(err)
	}
}
func determainJSON(JsonStr string) {
	var data interface{}
	// unmarshalling data into empty interface
	err := json.Unmarshal([]byte(JsonStr), &data)
	checkNilErr(err)
	// checking if data is array or object
	switch data.(type) {
	case []interface{}:
		fmt.Println("Array of object ")
	case map[string]interface{}:
		fmt.Println("Object")
		check(JsonStr)
	default:
		fmt.Println("Unknown")
	}
}

func check(jsonData string) {
	// Simulated JSON data
	// jsonData := `{"id":2,"firstName":"Sheldon","lastName":"Quigley","maidenName":"Cole","age":28,"gender":"male","email":"hbingley1@plala.or.jp","phone":"+7 813 117 7139","username":"hbingley1","password":"CQutx25i8r","birthDate":"2003-08-02","image":"https://robohash.org/Sheldon.png?set=set4","bloodGroup":"O+","height":187,"weight":74,"eyeColor":"Brown","hair":{"color":"Blond","type":"Curly"},"domain":"51.la","ip":"253.240.20.181","address":{"address":"6007 Applegate Lane","city":"Louisville","coordinates":{"lat":38.1343013,"lng":-85.6498512},"postalCode":"40219","state":"KY"},"macAddress":"13:F1:00:DA:A4:12","university":"Stavropol State Technical University","bank":{"cardExpire":"10/23","cardNumber":"5355920631952404","cardType":"mastercard","currency":"Ruble","iban":"MD63 L6YC 8YH4 QVQB XHIK MTML"},"company":{"address":{"address":"8821 West Myrtle Avenue","city":"Glendale","coordinates":{"lat":33.5404296,"lng":-112.2488391},"postalCode":"85305","state":"AZ"},"department":"Services","name":"Aufderhar-Cronin","title":"Senior Cost Accountant"},"ein":"52-5262907","ssn":"447-08-9217","userAgent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/534.30 (KHTML, like Gecko) Ubuntu/11.04 Chromium/12.0.742.112 Chrome/12.0.742.112 Safari/534.30","crypto":{"coin":"Bitcoin","wallet":"0xb9fc2fe63b2a6c003f1c324c3bfa53259162181a","network":"Ethereum (ERC20)"}}`

	// Define an empty interface{} to hold the decoded JSON data
	var data interface{}

	// Unmarshal JSON data into the empty interface{}
	err := json.Unmarshal([]byte(jsonData), &data)
	if err != nil {
		fmt.Printf("Error unmarshaling JSON: %v\n", err)
		return
	}
	var df_json dataframe.DataFrame
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
		// creating the dataframe with out nested json data
		jsonstr := "[{" + strings.Join(keys, ",") + "}]"
		fmt.Println(jsonstr)
		df_outjson := dataframe.ReadJSON(strings.NewReader(jsonstr))

		fmt.Println(df_outjson)
		// fmt.Println(df_json)
		final_df := df_json.CrossJoin(df_outjson)

		fmt.Println(final_df)
		toCSV(final_df, "LakshnamTest.csv")

	}
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
	file, err := os.Create(filename)
	checkNilErr(err)
	defer file.Close()
	df.WriteCSV(file)
	fmt.Println("hammayaa....\nCSV file created successfully.")
}
