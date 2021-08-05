package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/hashsearch"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/streamject"
	. "github.com/gagliardetto/utilz"
	"github.com/joho/godotenv"
	"github.com/mr-tron/base58"
)

var (
	bonfidaAuctionProgramKey = solana.MustPublicKeyFromBase58("AVWV7vdWbLqXiLKFaP19GhYurhwxaLp2qRBSjT5tR5vT")
	bonfidaNamesProgramKey   = solana.MustPublicKeyFromBase58("namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX")
	isProd                   bool
	domainsFileDir           string
	domainsFileName          string
	fileMode                 uint64
)

func main() {
	envPath := flag.String("env", ".env", "path for environment variables")
	flag.Parse()
	var err error
	if err = godotenv.Load(*envPath); err != nil {
		log.Fatalln("Error loading environment variables: ", err)
	}
	isProdS := os.Getenv("IS_PROD")
	isProd, err = strconv.ParseBool(isProdS)
	checkError(err)

	domainsFileDir = os.Getenv("DOMAINS_FILE_DIR")
	domainsFileName = os.Getenv("DOMAINS_FILE_NAME")
	fileModeS := os.Getenv("FILE_MODE")
	fileMode, err = strconv.ParseUint(fileModeS, 8, 32)
	checkError(err)

	MustCreateFolderIfNotExists(domainsFileDir, fs.FileMode(fileMode))

	rpcWithRate := rpc.NewWithRateLimit(
		os.Getenv("RPC_ENDPOINT"),
		3,
	)

	path := filepath.Join(domainsFileDir, domainsFileName)
	stm, err := streamject.NewJSON(path)
	checkError(err)
	defer stm.Close()

	c := rpc.NewWithCustomRPCClient(rpcWithRate)
	run(c, stm)
}

func run(client *rpc.Client, stm *streamject.Stream) {
	out, err := client.GetProgramAccountsWithOpts(
		context.Background(),
		bonfidaAuctionProgramKey,
		&rpc.GetProgramAccountsOpts{
			Encoding: solana.EncodingJSONParsed,
		},
	)
	checkError(err)

	targets := []solana.PublicKey{}
	for accountIndex := range out {
		targets = append(targets,
			out[accountIndex].Pubkey,
		)
	}

	Sfln(Shakespeare("%v targets"), len(targets))
	if isProd {
		time.Sleep(time.Second * 5)
	}

	for _, auctionPubkey := range targets {
		Sfln(OrangeBG("%s"), auctionPubkey)
		if isProd && hasByAuctionKey(stm, auctionPubkey) {
			Sfln("%s auctionPubkey already done; skipping", auctionPubkey)
			continue
		}
		err := processAuction(stm, client, auctionPubkey, isProd)
		if err != nil {
			if strings.Contains(err.Error(), "Connection rate limits exceeded") {
				Sfln("%s auctionPubkey is being ratelimited: %s", auctionPubkey, err)
				time.Sleep(time.Minute)
				continue
			}
			Sfln(Red("err while %s: %s"), auctionPubkey, err)
			err = stm.Append(&EmptyItem{
				AuctionKey: auctionPubkey,
			})
			checkError(err)
		}
	}

	// TODO:
	// - GetProgramAccountsWithOpts for AVWV7vdWbLqXiLKFaP19GhYurhwxaLp2qRBSjT5tR5vT as auctionAccounts
	// - foreach auctionAccounts as auctionAccount
	// 		- get transactions for auctionAccount
	//    	- get first transaction: get inner instructions; last inner instruction on(namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX) contains the name
	// 		- after last 00 00 00 comes the domain
}

func processAuction(
	stm *streamject.Stream,
	client *rpc.Client,
	auctionPubkey solana.PublicKey,
	isProd bool,
) error {
	Sfln(Shakespeare("----- %s"), "auctionPubkey above")

	ctx := context.Background()
	signatures, err := client.GetConfirmedSignaturesForAddress2(
		ctx,
		auctionPubkey,
		nil,
	)
	if err != nil {
		return err
	}

	spew.Dump(signatures)
	Sfln(Shakespeare("----- %s"), "signatures above")

	if len(signatures) == 0 {
		return errors.New("no signatures")
	}

	var domainItem DomainItem
	// Figure out what is the domain in the auction (if any):
	{
		oldestSignature := signatures[len(signatures)-1]
		spew.Dump(oldestSignature)
		Sfln(Shakespeare("----- %s"), "oldestSignature above")

		if oldestSignature.Err != nil {
			return errors.New("oldestSignature has err")
		}

		tx, err := client.GetConfirmedTransaction(
			ctx,
			oldestSignature.Signature,
		)
		if err != nil {
			return err
		}
		spew.Dump(tx)
		Sfln(Shakespeare("----- %s"), "tx above")

		if !SliceContains(tx.Meta.LogMessages, "Program log: Instruction: Create") {
			return errors.New("NOT: Instruction: Create")
		}
		if !SliceContains(tx.Meta.LogMessages, "Program jCebN34bUfdeUYJT13J1yG16XWQpt5PDx6Mse9GUqhR success") {
			return errors.New("NOT: Program jCebN34bUfdeUYJT13J1yG16XWQpt5PDx6Mse9GUqhR success")
		}
		firstIntruction := tx.Meta.InnerInstructions[0]

		lastInnerIndex := len(firstIntruction.Instructions) - 1
		lastInner := firstIntruction.Instructions[lastInnerIndex]
		Sfln("inner %v", lastInnerIndex+1)
		programKey := tx.Transaction.Message.AccountKeys[lastInner.ProgramIDIndex]
		Sfln(" program %s", programKey)
		if !bonfidaNamesProgramKey.Equals(programKey) {
			return errors.New("last instruction is not on bonfidaNamesProgramKey")
		}
		for _, accIndex := range lastInner.Accounts {
			Sfln(" account %s", tx.Transaction.Message.AccountKeys[int(accIndex)])
		}
		un, err := base58.Decode(lastInner.Data.String())
		if err != nil {
			return err
		}
		spew.Dump(([]byte(un)))

		parts := bytes.Split([]byte(un), []byte{00, 00, 00})
		name := parts[len(parts)-1]
		spew.Dump(name)

		creator := tx.Transaction.Message.AccountKeys[0]
		blockTimeInt64 := int64(*oldestSignature.BlockTime)
		Sfln(
			ShakespeareBG("auction for %q started by %s (slot:%v,blockTime:%s, %v)"),
			name,
			creator,
			oldestSignature.Slot,
			oldestSignature.BlockTime.Time(),
			blockTimeInt64,
		)
		domainItem = DomainItem{
			AuctionKey:     auctionPubkey,
			DomainName:     string(name),
			Creator:        creator,
			Slot:           uint64(oldestSignature.Slot),
			BlockTime:      oldestSignature.BlockTime.Time(),
			BlockTimeInt64: blockTimeInt64,
			FirstTx:        oldestSignature.Signature,
		}
	}

	// Find the latest bid (if any):
	if len(signatures) > 1 {
		canceledBids := make(map[string]bool)
		for i := 0; i < len(signatures)-1; i++ {
			mostRecentSignature := signatures[i]
			spew.Dump(mostRecentSignature)
			Sfln(Shakespeare("----- %s"), "mostRecentSignature above")
			if mostRecentSignature.Err != nil {
				continue
			}

			tx, err := client.GetConfirmedTransaction(
				ctx,
				mostRecentSignature.Signature,
			)
			if err != nil {
				return err
			}
			spew.Dump(tx)
			Sfln(Shakespeare("----- %s"), "tx above")
			// bidder := tx.Transaction.Message.AccountKeys[postTokenBalance.AccountIndex]
			bidder := tx.Transaction.Message.AccountKeys[0]

			if SliceContains(tx.Meta.LogMessages, "Program log: + Processing Cancelbid") {
				if len(tx.Meta.PreTokenBalances) == 0 || len(tx.Meta.PostTokenBalances) == 0 {
					Ln("warn: no PreTokenBalances or PostTokenBalances")
				} else {
					// Remember canceled bid:
					preTokenBalance := tx.Meta.PreTokenBalances[0]
					postTokenBalance := tx.Meta.PostTokenBalances[0]
					bidAmount := mustParseInt64(postTokenBalance.UiTokenAmount.Amount) - mustParseInt64(preTokenBalance.UiTokenAmount.Amount)

					bidCancelID := Sf("%s:%v", bidder, math.Abs(float64(bidAmount)))
					canceledBids[bidCancelID] = true
				}
				continue
			}
			if SliceContains(tx.Meta.LogMessages, "Program log: Instruction: Claim") {
				continue
			}
			if SliceContains(tx.Meta.LogMessages, "Program log: Auction ended!") {
				continue
			}
			if !SliceContains(tx.Meta.LogMessages, "Program log: + Processing PlaceBid") ||
				!SliceContains(tx.Meta.LogMessages, "Program AVWV7vdWbLqXiLKFaP19GhYurhwxaLp2qRBSjT5tR5vT success") {
				continue
			}
			// TODO: is there a guarantee/certainty that the last successful bid is also the highest?
			// TODO: the highest bid might also have been canceled

			if len(tx.Meta.PreTokenBalances) == 0 || len(tx.Meta.PostTokenBalances) == 0 {
				Ln("warn: no PreTokenBalances or PostTokenBalances")
				continue
			}
			preTokenBalance := tx.Meta.PreTokenBalances[0]
			postTokenBalance := tx.Meta.PostTokenBalances[0]

			bidAmount := mustParseInt64(preTokenBalance.UiTokenAmount.Amount) - mustParseInt64(postTokenBalance.UiTokenAmount.Amount)

			bidCancelID := Sf("%s:%v", bidder, math.Abs(float64(bidAmount)))
			if _, ok := canceledBids[bidCancelID]; ok {
				Ln("warn: this bid was canceled")
				continue
			}

			blockTimeInt64 := int64(*mostRecentSignature.BlockTime)
			Sfln(
				" latest offer from account %s for %v USDC",
				bidder,
				bidAmount,
			)
			// TODO:
			// - check that it's a bid, and not a revoked bid
			domainItem.MaxBid.Amount = bidAmount
			domainItem.MaxBid.Bidder = bidder
			domainItem.MaxBid.Slot = uint64(mostRecentSignature.Slot)
			domainItem.MaxBid.BlockTime = mostRecentSignature.BlockTime.Time()
			domainItem.MaxBid.BlockTimeInt64 = blockTimeInt64
			domainItem.MaxBid.Tx = mostRecentSignature.Signature
			break
		}
	} else {
		Sfln("warn: No offers at the moment")
	}

	return stm.Append(&domainItem)
}

func hasByAuctionKey(stm *streamject.Stream, auctionPubkey solana.PublicKey) bool {

	indexName := "auction.pubkey"

	stm.CreateIndexByUint64(indexName, func(line streamject.Line) uint64 {
		var build DomainItem
		err := line.Decode(&build)
		if err != nil {
			panic(err)
		}

		return hashsearch.HashString(build.AuctionKey.String())
	})

	return stm.HasUint64ByIndex(indexName, hashsearch.HashString(auctionPubkey.String()))
}

type EmptyItem struct {
	AuctionKey solana.PublicKey
}

type DomainItem struct {
	AuctionKey     solana.PublicKey
	DomainName     string // TODO: save as hex?
	Creator        solana.PublicKey
	Slot           uint64
	BlockTime      time.Time
	BlockTimeInt64 int64
	FirstTx        solana.Signature
	MaxBid         struct {
		Amount         int64
		Bidder         solana.PublicKey
		Slot           uint64
		BlockTime      time.Time
		BlockTimeInt64 int64
		Tx             solana.Signature
	}
}

func mustParseInt64(s string) int64 {
	out, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return out
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
