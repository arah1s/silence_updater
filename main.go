package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/opesun/goquery"
	"log"
	"math/rand"
	"strconv"
	"strings"
)

const (
	urlVPustotu                = "http://vpustotu.ru/story/"
	urlKillMePls               = "https://killpls.me/story/"
	urlPodslushano             = "https://ideer.ru/"
	urlNefart                  = "http://nefart.ru/"
	serviceNameVPustotuInDB    = "v pustotu"
	serviceNameKillMePlsInDB   = "kill me please"
	serviceNamePodslushanoInDB = "podslushano"
	serviceNameNefartInDB      = "nefart"

	dbHost     = "localhost"
	dbPort     = "5432"
	dbUser     = "god"
	dbPassword = "kartoshka"
	dbName     = "secretdb"
)

type Resource struct {
	id     int
	name   string
	url    string
	active bool
}

type Post struct {
	id       int
	likes    int
	dislikes int
	resource string
	text     string
}

func main() {
	dbConnectString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)
	db, err := sql.Open("postgres", dbConnectString)
	defer db.Close()
	if err != nil {
		log.Printf("[ERROR] Database opening error -->%v\n", err)
		panic("Database error")
	}

	/*	dbConnectString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			dbHost, dbPort, dbUser, dbPassword, dbName)
		db, err := sql.Open("postgres", dbConnectString)
		defer db.Close()
		if err != nil {
			log.Printf("[ERROR] Database opening error -->%v\n", err)
			panic("Database error")
		}


		rows, err := db.Query("SELECT * FROM resources")
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		resources := make([]*Resource, 0)
		for rows.Next() {
			resource := new(Resource)
			err := rows.Scan(&resource.id, &resource.name, &resource.url, &resource.active)
			if err != nil {
				log.Fatal(err)
			}
			resources = append(resources, resource)
		}


		for _, resource := range resources {
			fmt.Printf("%d, %s, %s, $v\n", resource.id, resource.name, resource.url, resource.active)
		}*/

	/*post := getRandomPostFromDB(db)
	fmt.Println(post.text)*/


	postsFromPustota := getPostsFromPustota()
	addPostInDB(db, postsFromPustota, serviceNameVPustotuInDB)

	/*postsFromKillMePls := getPostsFromKillMePls()
	addPostInDB(db, postsFromKillMePls, serviceNameKillMePlsInDB)

	postsFromPodslushano := getPostsFromPodslushano()
	addPostInDB(db, postsFromPodslushano, serviceNamePodslushanoInDB)

	postsFromNefart := getPostsFromNefart()
	addPostInDB(db, postsFromNefart, serviceNameNefartInDB)*/


	fmt.Println(postsFromPustota)
	//fmt.Println(postsFromKillMePls)
	//fmt.Println(postsFromPodslushano)
	//fmt.Println(postsFromNefart)





}

//addPostInDB uppload posts in DB
func addPostInDB(db *sql.DB, posts []string, resourceName string) {
	//get resource ID from table resources
	query := fmt.Sprintf("select id from resources where name = '%s'", resourceName)
	resourceID, _ := strconv.Atoi(getStringFromDB(db, query))

	//get count rows in table posts before upload content
	query = fmt.Sprintf("select count(*) from posts where resource_id = %d", resourceID)
	countRowsBeforeUploadPosts, _ := strconv.Atoi(getStringFromDB(db, query))

	for i := 0; i < len(posts); i++ {
		query = "select max(id) from posts"
		maxID, _ := strconv.Atoi(getStringFromDB(db, query))
		hash := GetMD5Hash(posts[i])
		query = fmt.Sprintf("select hash from post_description where hash = '%s'", hash)
		hashInDB := getStringFromDB(db, query)
		//add only new posts
		if hashInDB == "" {
			query = fmt.Sprintf(`insert into posts(id, resource_id, active, likes, dislikes) values (%d, %d, true, 0, 0);`+
				`insert into post_description (post_id, text, hash) values (%d, '%s', '%s')`,
				maxID+1, resourceID, maxID+1, posts[i], hash)
			err := addRowInDB(db, query)
			if err != nil {
				fmt.Println("[ERROR]: Error insert row in DB :", err)
			}
		}
	}

	//get count rows in table posts after upload content
	query = fmt.Sprintf("select count(*) from posts where resource_id = %d", resourceID)
	countRowsAfterUploadPosts, _ := strconv.Atoi(getStringFromDB(db, query))

	fmt.Println("[LOG]: Load ", (countRowsAfterUploadPosts - countRowsBeforeUploadPosts), " posts from '"+resourceName+"'")
}

func addRowInDB(db *sql.DB, query string) (err error){
	rows, err := db.Query(query)
	if err != nil {
		fmt.Println("[ERROR]: Error insert row in DB error:", err)
		fmt.Println("[ERROR]: Error insert row in DB query:", query)
	}
	defer rows.Close()
	return err
}

//getStringFromDB extract one string from db
func getStringFromDB(db *sql.DB, query string) (result string) {
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&result)
		if err != nil {
			log.Fatal(err)
		}
	}
	return result
}

//getRandomPostFromDB
func getRandomPostFromDB(db *sql.DB) (post Post) {
	query := "select max(id) from posts"
	maxID, _ := strconv.Atoi(getStringFromDB(db, query))
	randomID := rand.Intn(maxID)
	query = fmt.Sprintf("select posts.id, likes, dislikes, url, text from posts "+
		"join resources on posts.resource_id = resources.id "+
		"join post_description on posts.id = post_description.post_id "+
		"where posts.active = true and resources.active = true and posts.id >= %d limit 1;", randomID)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&post.id, &post.likes, &post.dislikes, &post.resource, &post.text)
		if err != nil {
			log.Fatal(err)
		}
	}

	return post
}

//GetMD5Hash calculate hash for post
func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

//getPostsFromNefart scrap site Nefart
func getPostsFromNefart() (postsFromFromNefart []string) {
	response, err := goquery.ParseUrl(urlNefart)
	if err != nil {
		log.Printf("[ERROR] Can not get response from %s error:%s", urlNefart, err)
	}
	firstPost := strings.Split(strings.TrimSpace(response.Find(".postpad").First().Text()), "\n")

	for i := 0; i < 80000; i++ {
	//for i := 40000; i < 45000; i++ {
		//fmt.Println(i)
		url := fmt.Sprintf("%s%d", urlNefart, i)
		response, err := goquery.ParseUrl(url)
		if err != nil {
			log.Printf("[ERROR] Can not get response from %s error:%s", url, err)
		}

		partWithPost := strings.TrimSpace(response.Find(".postpad").First().Text())
		post := strings.Split(partWithPost, "\n")

		if post[0] != "" && post[0] != firstPost[0] {
			post[0] = strings.Replace(post[0], `"`, `&#34;`, -1)
			post[0] = strings.Replace(post[0], "'", `&#39;`, -1)
			postsFromFromNefart = append(postsFromFromNefart, post[0])
		}
	}
	return postsFromFromNefart
}

//getPostsFromPodslushano scrap site Podslushano
func getPostsFromPodslushano() (postsFromPodslushano []string) {
	response, err := goquery.ParseUrl(urlPodslushano)
	if err != nil {
		log.Printf("[ERROR] Can not get response from %s error:%s", urlPodslushano, err)
	}
	firstPost := response.Find(".shortContent").Html()

	for i := 1; i < 100000; i++ {
		//fmt.Println(i)
		url := fmt.Sprintf("%s%d", urlPodslushano, i)
		response, err := goquery.ParseUrl(url)
		if err != nil {
			log.Printf("[ERROR] Can not get response from %s error:%s", url, err)
		}

		post := response.Find(".shortContent").Html()
		if post != "" && post != firstPost {
			//post = strings.Replace(post, "&#34;", `"`, -1)
			//post = strings.Replace(post, "&#39;", `'`, -1)
			post = strings.Replace(post, "&gt;", `>`, -1)
			post = strings.Replace(post, "&lt;", `<`, -1)
			post = strings.TrimSpace(strings.Replace(post, "<br/>", " ", -1))
			postsFromPodslushano = append(postsFromPodslushano, post)
		}
	}
	return postsFromPodslushano
}

//getPostsFromKillMePls scrap site KillMePls
func getPostsFromKillMePls() (postsFromKillMePls []string) {
	for i := 0; i < 10000; i++ {
		url := fmt.Sprintf("%s%d", urlKillMePls, i)
		response, err := goquery.ParseUrl(url)
		if err != nil {
			log.Printf("[ERROR] Can not get response from %s error:%s", url, err)
		}

		post := response.Find(".col-xs-12").Html()

		if post != "" {
			//post = strings.Replace(post, "&#34;", `"`, -1)
			//post = strings.Replace(post, "&#39;", `'`, -1)
			post = strings.TrimSpace(strings.Replace(post, "<br/>", " ", -1))
			postsFromKillMePls = append(postsFromKillMePls, post)
		}
	}
	return postsFromKillMePls
}

//getPostsFromPustota scrap site Pustota
func getPostsFromPustota() (postsFromPustota []string) {
	for i := 0; i < 10000; i++ {
		url := fmt.Sprintf("%s%d", urlVPustotu, i)
		response, err := goquery.ParseUrl(url)
		if err != nil {
			log.Printf("[ERROR] Can not get response from %s error:%s", url, err)
		}

		//if in response no button "Следующий →", this page empty
		if strings.Contains(response.Text(), "Следующий →") {
			post := response.Find(".fi_text").Text()
			trimString := fmt.Sprintf("%d%s", i, "@vpustotu.ru ")
			post = strings.Replace(post, `"`, `&#34;`, -1)
			post = strings.Replace(post, "'", `&#39;`, -1)
			post = strings.TrimSpace(strings.TrimLeft(post, trimString))
			postsFromPustota = append(postsFromPustota, post)
		}
	}
	return postsFromPustota
}
