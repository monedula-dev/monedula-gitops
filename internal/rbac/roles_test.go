package rbac

import "testing"

func TestClassifyRole_ClusterScoped(t *testing.T) {
	clusterScoped := []string{
		"SystemAdmin",
		"ClusterAdmin",
		"UserAdmin",
		"Operator",
		"SecurityAdmin",
		"AuditAdmin",
	}
	for _, role := range clusterScoped {
		kind, ok := ClassifyRole(role)
		if !ok {
			t.Errorf("ClassifyRole(%q) known=false, want true", role)
		}
		if kind != RoleClusterScoped {
			t.Errorf("ClassifyRole(%q) kind=%v, want RoleClusterScoped", role, kind)
		}
	}
}

func TestClassifyRole_ResourceScoped(t *testing.T) {
	resourceScoped := []string{
		"ResourceOwner",
		"DeveloperRead",
		"DeveloperWrite",
		"DeveloperManage",
	}
	for _, role := range resourceScoped {
		kind, ok := ClassifyRole(role)
		if !ok {
			t.Errorf("ClassifyRole(%q) known=false, want true", role)
		}
		if kind != RoleResourceScoped {
			t.Errorf("ClassifyRole(%q) kind=%v, want RoleResourceScoped", role, kind)
		}
	}
}

func TestClassifyRole_Unknown(t *testing.T) {
	unknown := []string{
		"",
		"Admin",
		"developerread",
		"SYSTEMADMIN",
		"NotARealRole",
	}
	for _, role := range unknown {
		kind, ok := ClassifyRole(role)
		if ok {
			t.Errorf("ClassifyRole(%q) known=true, want false", role)
		}
		if kind != RoleUnknown {
			t.Errorf("ClassifyRole(%q) kind=%v, want RoleUnknown", role, kind)
		}
	}
}
