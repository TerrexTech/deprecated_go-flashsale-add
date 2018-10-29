package flash

import (
	"log"
	"math"
	"time"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/pkg/errors"
)

type ConfigSchema struct {
	Flash     *Flash
	Metric    *Metric
	Inventory *Inventory
}

type DBIConfig struct {
	Hosts               []string
	Username            string
	Password            string
	TimeoutMilliseconds uint32
	Database            string
	Collection          string
}

type DBI interface {
	Collection() *mongo.Collection
	AddFlashSale(flash []Flash) ([]*mgo.InsertOneResult, error)
}

type DB struct {
	collection *mongo.Collection
}

func GenerateDB(dbConfig DBIConfig, schema interface{}) (*DB, error) {
	config := mongo.ClientConfig{
		Hosts:               dbConfig.Hosts,
		Username:            dbConfig.Username,
		Password:            dbConfig.Password,
		TimeoutMilliseconds: dbConfig.TimeoutMilliseconds,
	}

	client, err := mongo.NewClient(config)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}

	conn := &mongo.ConnectionConfig{
		Client:  client,
		Timeout: 5000,
	}

	// indexConfigs := []mongo.IndexConfig{
	// 	mongo.IndexConfig{
	// 		ColumnConfig: []mongo.IndexColumnConfig{
	// 			mongo.IndexColumnConfig{
	// 				Name: "item_id",
	// 			},
	// 		},
	// 		IsUnique: true,
	// 		Name:     "item_id_index",
	// 	},
	// 	mongo.IndexConfig{
	// 		ColumnConfig: []mongo.IndexColumnConfig{
	// 			mongo.IndexColumnConfig{
	// 				Name:        "timestamp",
	// 				IsDescOrder: true,
	// 			},
	// 		},
	// 		IsUnique: true,
	// 		Name:     "timestamp_index",
	// 	},
	// }

	// ====> Create New Collection
	collConfig := &mongo.Collection{
		Connection:   conn,
		Database:     dbConfig.Database,
		Name:         dbConfig.Collection,
		SchemaStruct: schema,
		// Indexes:      indexConfigs,
	}
	c, err := mongo.EnsureCollection(collConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}
	return &DB{
		collection: c,
	}, nil
}

func (d *DB) Collection() *mongo.Collection {
	return d.collection
}

func (db *DB) SearchMetThreshold(threshold float64) (*[]Metric, error) {

	findResults, err := db.collection.Find(map[string]interface{}{
		"ethylene": map[string]float64{
			"$gte": threshold,
		},
	})

	if err != nil {
		err = errors.Wrap(err, "Error while searching Metrics db - SearchMetThreshold")
		log.Println(err)
		return nil, err
	}

	//length
	if len(findResults) == 0 {
		msg := "No results found - SearchByDate"
		return nil, errors.New(msg)
	}

	met := []Metric{}

	for _, v := range findResults {
		result := v.(*Metric)
		met = append(met, *result)
	}
	return &met, nil
}

func (db *DB) AddFlashSale(fsale []Flash) ([]*mgo.InsertOneResult, error) {
	var insertResult *mgo.InsertOneResult
	var getMultipleInserts []*mgo.InsertOneResult

	uuid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Unable to generate UUID")
		log.Println(err)
	}

	for _, v := range fsale {
		v.FlashID = uuid
		v.Timestamp = time.Now().Unix()

		if v.FlashID.String() == "00000000-0000-0000-0000-000000000000" {
			log.Println("FlashID is empty")
			return nil, errors.New("FlashID not found")
		}

		// log.Println(v.ItemID.String())

		// item := v.ItemID.String()

		if v.ItemID.String() == "00000000-0000-0000-0000-000000000000" {
			log.Println("ItemID is empty")
			return nil, errors.New("ItemID not found")
		}

		if v.UPC == 0 {
			log.Println("UPC is empty")
			return nil, errors.New("UPC not found")
		}

		if v.SKU == 0 {
			log.Println("SKU is empty")
			return nil, errors.New("SKU not found")
		}

		if v.Name == "" {
			log.Println("Name is empty")
			return nil, errors.New("Name not found")
		}

		if v.Origin == "" {
			log.Println("Origin is empty")
			return nil, errors.New("Origin not found")
		}

		if v.DeviceID.String() == "00000000-0000-0000-0000-000000000000" {
			log.Println("DeviceID is empty")
			return nil, errors.New("DeviceID not found")
		}

		if v.Price == 0 {
			log.Println("Price is empty")
			return nil, errors.New("Price not found")
		}

		if v.SalePrice < 0 || v.SalePrice > math.MaxInt64 {
			log.Println("Sale Price error. Number is either less than 0 or greater than allowed max value")
			return nil, errors.New("Sale Price not found")
		}

		if v.Ethylene == 0 {
			log.Println("Ethylene value is empty")
			return nil, errors.New("Ethylene not found")
		}

		insertResult, err = db.collection.InsertOne(&v)
		if err != nil {
			err = errors.Wrap(err, "Unable to insert into Flash sale")
			log.Println(err)
			return nil, err
		}

		log.Println(insertResult.InsertedID)

		getMultipleInserts = append(getMultipleInserts, insertResult)
	}

	return getMultipleInserts, nil

}
