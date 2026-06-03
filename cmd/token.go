package cmd

// token.go provides operator-only CLI commands for managing the
// bearer tokens of a multi-tenant batchq server. Same caveats as
// tenant.go: operator-only, opens storage directly, must be run on
// the server host.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/mbreese/batchq/storage"
	"github.com/mbreese/batchq/support"
	"github.com/spf13/cobra"
)

var (
	tokenMintTenant    string
	tokenMintLabel     string
	tokenMintExpiresIn time.Duration
	tokenListTenant    string
)

var tokenCmd = &cobra.Command{
	Use:   "token",
	Short: "Manage tenant bearer tokens (operator-only)",
}

var tokenMintCmd = &cobra.Command{
	Use:   "mint",
	Short: "Mint a new bearer token for a tenant",
	Long: `Generate a new bearer token for the given tenant and print it
to stdout. The token is shown ONCE — store it somewhere safe (a
password manager or BATCHQ_TOKEN env var) immediately; the server
only stores the HMAC, not the token itself, so it cannot reprint.

By default the token has no expiry. Pass --expires-in to set a
duration (e.g. --expires-in 720h for 30 days).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if tokenMintTenant == "" {
			return fmt.Errorf("--tenant is required")
		}
		store, err := openOperatorStore()
		if err != nil {
			return err
		}
		defer store.Close()
		t, err := resolveTenantArg(context.Background(), store, tokenMintTenant)
		if err != nil {
			return err
		}
		key, err := operatorMasterKey()
		if err != nil {
			return err
		}
		signer := support.NewTokenSigner(key)
		plaintext, hmac, err := signer.GenerateToken()
		if err != nil {
			return err
		}
		var expiresAt time.Time
		if tokenMintExpiresIn > 0 {
			expiresAt = time.Now().UTC().Add(tokenMintExpiresIn)
		}
		row, err := store.CreateToken(context.Background(), t.ID, hmac, tokenMintLabel, expiresAt)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Token minted for tenant %s (%s).\n", t.Name, t.ID)
		fmt.Fprintf(os.Stderr, "  id:    %s\n", row.ID)
		if tokenMintLabel != "" {
			fmt.Fprintf(os.Stderr, "  label: %s\n", tokenMintLabel)
		}
		if !expiresAt.IsZero() {
			fmt.Fprintf(os.Stderr, "  expires: %s\n", expiresAt.Format("2006-01-02 15:04:05 UTC"))
		} else {
			fmt.Fprintln(os.Stderr, "  expires: never")
		}
		fmt.Fprintln(os.Stderr, "\nThe token is printed below ONCE; the server only stores its HMAC.")
		fmt.Fprintln(os.Stderr, "Distribute it out-of-band (password manager, encrypted DM) — never commit.")
		fmt.Fprintln(os.Stderr)
		fmt.Println(plaintext)
		return nil
	},
}

var tokenListCmd = &cobra.Command{
	Use:   "list",
	Short: "List tokens for a tenant",
	RunE: func(cmd *cobra.Command, args []string) error {
		if tokenListTenant == "" {
			return fmt.Errorf("--tenant is required")
		}
		store, err := openOperatorStore()
		if err != nil {
			return err
		}
		defer store.Close()
		t, err := resolveTenantArg(context.Background(), store, tokenListTenant)
		if err != nil {
			return err
		}
		tokens, err := store.ListTokensForTenant(context.Background(), t.ID)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tLABEL\tCREATED\tEXPIRES\tSTATE")
		for _, tk := range tokens {
			state := "active"
			if !tk.RevokedAt.IsZero() {
				state = "revoked"
			} else if !tk.ExpiresAt.IsZero() && !time.Now().UTC().Before(tk.ExpiresAt) {
				state = "expired"
			}
			expires := "never"
			if !tk.ExpiresAt.IsZero() {
				expires = tk.ExpiresAt.Format("2006-01-02")
			}
			label := tk.Label
			if label == "" {
				label = "-"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				tk.ID, label, tk.CreatedAt.Format("2006-01-02"), expires, state)
		}
		return w.Flush()
	},
}

var tokenRevokeCmd = &cobra.Command{
	Use:   "revoke <token-id>",
	Short: "Revoke a bearer token by ID",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := openOperatorStore()
		if err != nil {
			return err
		}
		defer store.Close()
		if err := store.RevokeToken(context.Background(), args[0]); err != nil {
			if errors.Is(err, storage.ErrTokenNotFound) {
				return fmt.Errorf("no such token (or already revoked): %s", args[0])
			}
			return err
		}
		fmt.Fprintf(os.Stderr, "Revoked token %s.\n", args[0])
		return nil
	},
}

func init() {
	tokenMintCmd.Flags().StringVar(&tokenMintTenant, "tenant", "", "Tenant name or ID to mint a token for (required)")
	tokenMintCmd.Flags().StringVar(&tokenMintLabel, "label", "", "Optional label to identify this token in `token list`")
	tokenMintCmd.Flags().DurationVar(&tokenMintExpiresIn, "expires-in", 0, "Token lifetime; defaults to no expiry. e.g. 720h for 30 days")

	tokenListCmd.Flags().StringVar(&tokenListTenant, "tenant", "", "Tenant name or ID to list tokens for (required)")

	tokenCmd.AddCommand(tokenMintCmd)
	tokenCmd.AddCommand(tokenListCmd)
	tokenCmd.AddCommand(tokenRevokeCmd)
	rootCmd.AddCommand(tokenCmd)
}
