package main

import (
	"bytes"
	"database/sql"
	"github.com/gocolly/colly"
	"github.com/gocolly/colly/extensions"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO Refactore all code
// TODO сделать checked только после проверки, а также сохранение источника домена

func CreateDB() {
	// TODO check if table and db exists
	db, err := sql.Open("sqlite3", "./new_domains.db")
	if err != nil {
		log.Println(err)
	}
	defer db.Close()
	_, _ = db.Exec("CREATE TABLE IF NOT EXISTS `domains` (" +
		"`id` INTEGER PRIMARY KEY AUTOINCREMENT," +
		"`created` DATETIME DEFAULT CURRENT_TIMESTAMP," +
		"`domain` VARCHAR(256) NULL UNIQUE," +
		"`checked` BOOL DEFAULT FALSE" +
		");")
	ins, err := db.Prepare("INSERT OR IGNORE INTO domains(domain) values (?)")
	if err != nil {
		log.Println(err)
	}
	tx, err := db.Begin()
	if err != nil {
		log.Println(err)
	}
	_, _ = tx.Stmt(ins).Exec("yandex.ru")
	_, _ = tx.Stmt(ins).Exec("google.com")
	tx.Commit()
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
	var re = regexp.MustCompile(`(?mi)^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$`)
	c := colly.NewCollector(
		colly.MaxDepth(3),
		colly.MaxBodySize(31457280),
	)
	extensions.RandomUserAgent(c)
	extensions.Referer(c)
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		if len(link) > 3 {
			fullurl := e.Request.URL.Scheme + "://" + e.Request.URL.Host + "/"
			if link[:1] == "#" {

			} else if strings.HasPrefix(link, fullurl) {
				e.Request.Visit(link)
			} else if strings.HasPrefix(link, "/") && link[:2] != "//" {
				e.Request.Visit(link)
			} else {
				u, err := url.Parse(link)
				if err == nil {
					if re.MatchString(u.Hostname()) && strings.Contains(u.Hostname(), ".") {
						external <- u.Hostname()
					}
				}
			}
		}

	})
	c.Visit(target)
}

func SendAlert(db *sql.DB) {
	var count int
	var countTrue int
	var countFalse int
	t := time.Now()
	n := time.Date(t.Year(), t.Month(), t.Day(), 6, 0, 0, 0, t.Location())
	d := n.Sub(t)
	if d < 0 {
		n = n.Add(24 * time.Hour)
		d = n.Sub(t)
	}
	for {

		rows, err := db.Query("select count(*) from domains")
		for rows.Next() {
			err = rows.Scan(&count)
			if err != nil {
				log.Println(err)
			}
		}
		rows.Close()
		rows, err = db.Query("select count(*) from domains where checked=TRUE")
		for rows.Next() {
			err = rows.Scan(&countTrue)
			if err != nil {
				log.Println(err)
			}
		}
		rows.Close()
		rows, err = db.Query("select count(*) from domains where checked=FALSE")
		for rows.Next() {
			err = rows.Scan(&countFalse)
			if err != nil {
				log.Println(err)
			}
		}
		rows.Close()
		jsonStr := []byte(`{ "token": "` + Token + `", "message": "Всего записей: ` + strconv.Itoa(count) + `\nПроверенных записей: ` + strconv.Itoa(countTrue) + `\nНе проверенных записей: ` + strconv.Itoa(countFalse) + `"}`)
		urlReq := "http://192.168.88.215:9999/telegram"
		resp, err := http.Post(urlReq, "application/json", bytes.NewBuffer(jsonStr))
		if err != nil {
			log.Println(err)
		}
		defer resp.Body.Close()
		time.Sleep(d)
		d = 24 * time.Hour
	}
}

func AddTargets(db *sql.DB, targets chan<- string) {
	var domain string
	var domains []string
	upd, err := db.Prepare("update domains set checked=true where domain=?")
	if err != nil {
		log.Println(err)
	}
	rows, err := db.Query("select domain from domains where checked=false limit 300")
	for rows.Next() {
		err = rows.Scan(&domain)
		if err != nil {
			log.Println("SELECT DOMAINS", err)
		} else {
			targets <- "http://" + domain + "/"
			domains = append(domains, domain)
		}
	}
	rows.Close()
	for _, domain := range domains {
		_, err := upd.Exec(domain)
		if err != nil {
			log.Println("UPDATE DOMAINS", err)
		}
	}
	domains = []string{}
}

func StartParsing() {
	// TODO SQLITE and code optimisations
	var externalLinks []string
	db, err := sql.Open("sqlite3", "./new_domains.db")
	if err != nil {
		log.Println(err)
	}
	go SendAlert(db)
	ins, err := db.Prepare("INSERT OR IGNORE INTO domains(domain) values (?)")
	if err != nil {
		log.Println("PREPARE INSERT", err)
	}
	defer db.Close()
	targets := make(chan string, 1000)
	external := make(chan string, 10000)
	for i := 0; i < 230; i++ {
		go Worker(targets, external)
	}

	AddTargets(db, targets)
	for link := range external {
		if len(targets) < 300 {
			AddTargets(db, targets)
		}

		externalLinks = append(externalLinks, link)
		if len(externalLinks) > 50 {
			tx, err := db.Begin()
			if err != nil {
				log.Println("Transaction", err)
			}
			for _, link := range RemoveDuplicates(externalLinks) {
				_, err = tx.Stmt(ins).Exec(link)
				if err != nil {
					log.Println("Inserting link", err)
				}
			}
			tx.Commit()
			externalLinks = []string{}
		}
	}
}

func main() {
	// TODO add parse flags
	//CreateDB()
	StartParsing()
}
