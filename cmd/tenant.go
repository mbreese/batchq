package cmd

// tenant.go provides operator-only CLI commands for managing the
// tenants of a multi-tenant batchq server. These commands open the
// storage directly — they do NOT go through the REST API, because in
// v1 the API requires per-tenant authentication (chicken/egg for
// bootstrap). For remote deployments, run these on the server host.
//
// On a single-user local autospawn, you typically don't need these —
// the implicit local tenant is provisioned for you.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/mbreese/batchq/storage"
	"github.com/mbreese/batchq/support"
	"github.com/spf13/cobra"
)

var tenantCmd = &cobra.Command{
	Use:   "tenant",
	Short: "Manage tenants (operator-only; multi-tenant deployments)",
}

var tenantCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new remote (bearer-token) tenant",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := openOperatorStore()
		if err != nil {
			return err
		}
		defer store.Close()
		t, err := store.CreateTenant(context.Background(), args[0], storage.TenantKindRemote)
		if err != nil {
			if errors.Is(err, storage.ErrTenantExists) {
				return fmt.Errorf("tenant %q already exists", args[0])
			}
			return err
		}
		fmt.Fprintf(os.Stderr, "Tenant created.\n  id:   %s\n  name: %s\n  kind: %s\n", t.ID, t.Name, t.Kind)
		fmt.Fprintf(os.Stderr, "\nNext: batchq token mint --tenant %s\n", t.Name)
		return nil
	},
}

var tenantListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all tenants",
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := openOperatorStore()
		if err != nil {
			return err
		}
		defer store.Close()
		tenants, err := store.ListTenants(context.Background())
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tKIND\tCREATED")
		for _, t := range tenants {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", t.ID, t.Name, t.Kind, t.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		return w.Flush()
	},
}

var tenantDeleteCmd = &cobra.Command{
	Use:   "delete <name-or-id>",
	Short: "Delete a tenant (must have no jobs or tokens)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := openOperatorStore()
		if err != nil {
			return err
		}
		defer store.Close()
		t, err := resolveTenantArg(context.Background(), store, args[0])
		if err != nil {
			return err
		}
		if err := store.DeleteTenant(context.Background(), t.ID); err != nil {
			return fmt.Errorf("delete: %w (revoke any tokens and clean up jobs first)", err)
		}
		fmt.Fprintf(os.Stderr, "Deleted tenant %s (%s).\n", t.Name, t.ID)
		return nil
	},
}

// openOperatorStore opens the same sqlite DB the server uses, for
// the lifetime of one operator command. Concurrent access with a
// running server is fine on local disk (sqlite's file locks
// coordinate) but unsafe on a networked FS — same caveat as
// everywhere else in batchq.
func openOperatorStore() (storage.Storage, error) {
	backend, err := support.ParseBackend(Config.Server.DB)
	if err != nil {
		return nil, fmt.Errorf("parse [server] db: %w", err)
	}
	if backend.Scheme != support.BackendSqlite3 {
		return nil, fmt.Errorf("operator commands only support sqlite3 backends (got %s)", backend.Scheme)
	}
	path, err := backend.SqlitePath()
	if err != nil {
		return nil, err
	}
	return storage.Open(context.Background(), path, storage.Options{})
}

// operatorMasterKey loads (or creates) the master.key in
// $BATCHQ_HOME. Same path the server uses; operator commands and the
// server share the file.
func operatorMasterKey() ([]byte, error) {
	return support.LoadOrCreateMasterKey(filepath.Join(support.GetBatchqHome(), support.MasterKeyName))
}

// resolveTenantArg accepts either a tenant name or a tenant id and
// returns the corresponding *storage.Tenant.
func resolveTenantArg(ctx context.Context, store storage.Storage, arg string) (*storage.Tenant, error) {
	if t, err := store.GetTenantByName(ctx, arg); err == nil {
		return t, nil
	} else if !errors.Is(err, storage.ErrTenantNotFound) {
		return nil, err
	}
	t, err := store.GetTenantByID(ctx, arg)
	if err != nil {
		if errors.Is(err, storage.ErrTenantNotFound) {
			return nil, fmt.Errorf("no such tenant: %q", arg)
		}
		return nil, err
	}
	return t, nil
}

func init() {
	tenantCmd.AddCommand(tenantCreateCmd)
	tenantCmd.AddCommand(tenantListCmd)
	tenantCmd.AddCommand(tenantDeleteCmd)
	rootCmd.AddCommand(tenantCmd)
}
