package main

import (
	"github.com/gocolly/colly"
	"github.com/gocolly/colly/extensions"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"log"
	"net/url"
	"strings"
)

type Domains struct {
	gorm.Model
	Domain  string `gorm:"not null; size:255; unique; index"`
	Checked bool
}

func CreateDB() {
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
	//domain := Domains{Domain: "yandex.ru", Checked: false}
	//db.FirstOrCreate(&domain, Domains{Domain: "non_existing"})
	//domain = Domains{Domain: "qweqweqwe.ru", Checked: false}
	//db.FirstOrCreate(&domain, &domain)

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

func Parser(targets <-chan string, external chan<- string) {
	c := colly.NewCollector(
		colly.Async(true),
		colly.MaxDepth(3),
	)
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
				external <- link
			}
		}

	})
	for target := range targets {
		c.Visit(target)
		c.Wait()
	}
}

func StartParsing() {
	// TODO BULK INSERT AND OTHERS OPTIMIZATION
	//var insertDomains []interface{}
	var targetDomains []Domains
	var domain Domains
	var externalLinks []string
	db, err := gorm.Open("sqlite3", "domains.db")
	if err != nil {
		log.Println(err)
	}
	//db.LogMode(true)
	defer db.Close()
	targets := make(chan string, 10000)
	external := make(chan string, 10000)
	for i := 0; i < 5; i++ {
		go Parser(targets, external)
	}
	if len(targets) < 100 {
		db.Where("checked = ?", false).Limit(200).Find(&targetDomains)
		for _, domain := range targetDomains {
			targets <- "http://" + domain.Domain + "/"
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
		if len(externalLinks) > 1000 {
			for _, link := range RemoveDuplicates(externalLinks) {
				domain = Domains{Domain: link, Checked: false}
				db.FirstOrCreate(&domain, &domain)
			}
			externalLinks = []string{}
		}
	}
}

func main() {
	CreateDB()
	StartParsing()
}
