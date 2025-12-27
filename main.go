package main

import (
	"os"
	"bufio"
	"encoding/xml"
	"fmt"
	"net"
	// "net/netip"
	"slices"
	"strings"
	"regexp"
	"compress/gzip"
	"io"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/inserter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
)


type ArinOrg struct {
	XMLName xml.Name `xml:"org"`

	CountryCode2 string `xml:"iso3166-1>code2"`
	CountryCode3 string `xml:"iso3166-1>code3"`
	CountryName string `xml:"iso3166-1>name"`
	// CountryE164 int `xml:"iso3166-1>e164"`

	Handle string `xml:"handle"`
	ParentOrgHandle string `xml:"parentOrgHandle"`
}

type ArinNet struct {
	XMLName xml.Name `xml:"net"`

	OrgHandle string `xml:"orgHandle"`
	NetBlocks []*ArinNetBlock `xml:"netBlocks>netBlock"`
}

type ArinNetBlock struct {
	XMLName xml.Name `xml:"netBlock"`

	CidrLength int `xml:"cidrLenth"`
	StartAddress string `xml:"startAddress"`
	EndAddress string `xml:"endAddress"`
}

func main() {
	dbPath := "/Users/brien/urnetwork/config/temp/arindb/2025.12.27/arin_db.xml"

	reader := func()(*os.File, io.Reader, error) {
		f, err := os.Open(dbPath)
		if err != nil {
			return nil, nil, err
		}

		r := io.Reader(f)
		if strings.HasSuffix(dbPath, ".gz") {
			r, err = gzip.NewReader(r)
			if err != nil {
				f.Close()
				return nil, nil, err
			}
		}
		r = bufio.NewReader(r)
		return f, r, nil
	}

	// pass 1: create <org>
	// pass 2: map <net> to org and export

	orgs := map[string]*ArinOrg{}

	func() {
		f, r, err := reader()
		if err != nil {
			panic(err)
		}
		defer f.Close()

		d := xml.NewDecoder(r)

		parseOrgs := func() {
			for i := 0; ; i += 1 {
				t, err := d.Token()
				if err != nil {
					break
				}
				if i % 100 == 0 {
					fmt.Print(".")
				}
				switch v := t.(type) {
				case xml.StartElement:
					switch v.Name.Local {
					case "org":
						var arinOrg ArinOrg
						err = d.DecodeElement(&arinOrg, &v)
						if err != nil {
							panic(err)
						}
						fmt.Print("o")
						orgs[arinOrg.Handle] = &arinOrg
					default:
						err = d.Skip()
						if err != nil {
							panic(err)
						}
					}
				}
			}
		}


		for {
			t, err := d.Token()
			if err != nil {
				break
			}
			switch v := t.(type) {
			case xml.StartElement:
				switch v.Name.Local {
				case "bulkwhois":
					parseOrgs()
				default:
					err = d.Skip()
					if err != nil {
						panic(err)
					}
				}
			}
		}
	}()

	func() {
		f, r, err := reader()
		if err != nil {
			panic(err)
		}
		defer f.Close()

		d := xml.NewDecoder(r)


		// see https://blog.maxmind.com/2020/09/enriching-mmdb-files-with-your-own-data-using-go/
		writer, err := mmdbwriter.New(mmdbwriter.Options{
			DatabaseType: "urnetwork arindb",
		})
		if err != nil {
			panic(err)
		}


		addNet := func(arinNet *ArinNet) {
			for _, netBlock := range arinNet.NetBlocks {

				ip := sanitizeIp(netBlock.StartAddress)

				cidr := fmt.Sprintf("%s/%d", ip, netBlock.CidrLength)
				_, ipNet, err := net.ParseCIDR(cidr)
				if err != nil {
					fmt.Printf("Invalid net block. Skipping. cidr = %s\n", cidr)
				} else {
					// fmt.Printf("INSERT %s (%s)\n", ipNet, cidr)

					countryCodes := []string{}
					orgHandle := arinNet.OrgHandle
					for orgHandle != "" {
						org, ok := orgs[orgHandle]
						if !ok {
							break
						}
						countryCodes = append(countryCodes, strings.ToLower(org.CountryCode2))
						orgHandle = org.ParentOrgHandle
					}
					slices.Reverse(countryCodes)

					countryCodeDataTypes := []mmdbtype.DataType{}
					for _, countryCode := range countryCodes {
						countryCodeDataTypes = append(countryCodeDataTypes, mmdbtype.String(countryCode))
					}

					data := mmdbtype.Map{
						"org_country_codes": mmdbtype.Slice(countryCodeDataTypes),
					}

					err = writer.InsertFunc(ipNet, inserter.ReplaceWith(data))
					if err != nil {
						fmt.Printf("Failed to insert. Skipping. err = %s\n", err)
					}
				}

			}
		}


		parseNets := func() {
			for i := 0; ; i += 1 {
				t, err := d.Token()
				if err != nil {
					break
				}
				if i % 100 == 0 {
					fmt.Print(".")
				}
				switch v := t.(type) {
				case xml.StartElement:
					switch v.Name.Local {
					case "net":
						var net ArinNet
						err = d.DecodeElement(&net, &v)
						if err != nil {
							panic(err)
						}
						fmt.Print("n")
						addNet(&net)
					default:
						err = d.Skip()
						if err != nil {
							panic(err)
						}
					}
				}
			}
		}

		for {
			t, err := d.Token()
			if err != nil {
				break
			}
			switch v := t.(type) {
			case xml.StartElement:
				switch v.Name.Local {
				case "bulkwhois":
					parseNets()
				default:
					err = d.Skip()
					if err != nil {
						panic(err)
					}
				}
			}
		}


		outFile, err := os.Create("arin.mmdb")
		if err != nil {
			panic(err)
		}
		_, err = writer.WriteTo(outFile)
		if err != nil {
			panic(err)
		}
	}()
}


func sanitizeIp(ip string) string {
	if strings.Contains(ip, ":") {
		ipv6Truncate := regexp.MustCompile("((?::0+)+)$")
		return ipv6Truncate.ReplaceAllString(ip, "::")
	} else {
		var a int
		var b int
		var c int
		var d int
		fmt.Sscanf(ip, "%d.%d.%d.%d", &a, &b, &c, &d)
		return fmt.Sprintf("%d.%d.%d.%d", a, b, c, d)
	}

}
