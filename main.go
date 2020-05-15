package main

import (
	"bytes"
	"fmt"
	"github.com/gocolly/colly"
	"github.com/gocolly/colly/extensions"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO Refactore all code

type Domains struct {
	gorm.Model
	Domain  string `gorm:"not null; size:255; unique; index"`
	Checked bool
}

func CreateDB() {
	// TODO check if table and db exists
	db, err := gorm.Open("sqlite3", "domains.db")
	if err != nil {
		log.Println(err)
	}
	//db.LogMode(true)

	defer db.Close()

	// Migrate the schema
	db.AutoMigrate(&Domains{})
	db.Create(&Domains{Domain: "google.com", Checked: false})
	db.Create(&Domains{Domain: "yandex.ru", Checked: false})

}

func RemoveDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	var result []string

	for v := range elements {
		if encountered[elements[v]] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			encountered[elements[v]] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}

func Worker(targets <-chan string, external chan<- string) {
	var wg sync.WaitGroup
	for target := range targets {
		wg.Add(1)
		go Parser(target, external, &wg)
		wg.Wait()
	}
}

func Parser(target string, external chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	c := colly.NewCollector(
		colly.MaxDepth(3),
		colly.MaxBodySize(31457280),
	)
	// TODO Mb add limit parallelism
	extensions.RandomUserAgent(c)
	extensions.Referer(c)
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		if len(link) > 3 {
			fullurl := e.Request.URL.Scheme + "://" + e.Request.URL.Host + "/"
			if strings.HasPrefix(link, fullurl) || link[:1] == "#" {
				e.Request.Visit(link)
			} else if strings.HasPrefix(link, "/") && link[:2] != "//" {
				e.Request.Visit(link)
			} else {
				// TODO check if url is valid
				external <- link
			}
		}

	})
	c.Visit(target)
	//c.Wait()
}

func SendAlert(db *gorm.DB) {
	var result int
	t := time.Now()
	n := time.Date(t.Year(), t.Month(), t.Day(), 6, 0, 0, 0, t.Location())
	d := n.Sub(t)
	if d < 0 {
		n = n.Add(24 * time.Hour)
		d = n.Sub(t)
	}
	for {
		time.Sleep(d)
		d = 24 * time.Hour
		db.Table("domains").Count(&result)
		jsonStr := []byte(`{ "token": "` + Token + `", "message": "Всего записей в DB: ` + strconv.Itoa(result) + `"}`)
		urlReq := "http://192.168.88.215:9999/telegram"
		resp, err := http.Post(urlReq, "application/json", bytes.NewBuffer(jsonStr))
		if err != nil {
			log.Println(err)
		}
		defer resp.Body.Close()
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		fmt.Println(string(bodyBytes))
	}
}

func StartParsing() {
	// TODO BULK INSERT AND OTHERS OPTIMIZATIONS
	var targetDomains []Domains
	var domain Domains
	var externalLinks []string
	db, err := gorm.Open("sqlite3", "domains.db")
	if err != nil {
		log.Println(err)
	}
	go SendAlert(db)
	// TODO defer db.close()??
	//db.LogMode(true)
	//defer db.Close()
	targets := make(chan string, 1000)
	external := make(chan string, 10000)
	for i := 0; i < 100; i++ {
		go Worker(targets, external)
	}
	if len(targets) < 100 {
		db.Where("checked = ?", false).Limit(200).Find(&targetDomains)
		for _, domain := range targetDomains {
			targets <- "http://" + domain.Domain + "/"
			// TODO Update with select in one query
			db.Model(&domain).Update("checked", true)
		}
		targetDomains = []Domains{}
	}
	for link := range external {
		if len(targets) < 100 {
			db.Where("checked = ?", false).Limit(200).Find(&targetDomains)
			for _, domain := range targetDomains {
				targets <- "http://" + domain.Domain + "/"
				db.Model(&domain).Update("checked", true)
			}
			targetDomains = []Domains{}
		}
		u, err := url.Parse(link)
		if err != nil {
			continue
		}
		externalLinks = append(externalLinks, u.Hostname())
		if len(externalLinks) > 8000 {
			for _, link := range RemoveDuplicates(externalLinks) {
				domain = Domains{Domain: link, Checked: false}
				// TODO Add transaction to create
				db.FirstOrCreate(&domain, &domain)
			}
			externalLinks = []string{}
		}
	}
}

func main() {
	// TODO add parse flags
	CreateDB()
	StartParsing()
}
