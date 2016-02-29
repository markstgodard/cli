package application

import (
	"time"

	"github.com/cloudfoundry/cli/cf/api/logs"
	"github.com/cloudfoundry/cli/cf/command_registry"
	"github.com/cloudfoundry/cli/cf/configuration/core_config"
	"github.com/cloudfoundry/cli/cf/errors"
	. "github.com/cloudfoundry/cli/cf/i18n"
	"github.com/cloudfoundry/cli/cf/models"
	"github.com/cloudfoundry/cli/cf/requirements"
	"github.com/cloudfoundry/cli/cf/terminal"
	"github.com/cloudfoundry/cli/flags"
)

type Logs struct {
	ui       terminal.UI
	config   core_config.Reader
	logsRepo logs.LogsRepository
	appReq   requirements.ApplicationRequirement
}

func init() {
	command_registry.Register(&Logs{})
}

func (cmd *Logs) MetaData() command_registry.CommandMetadata {
	fs := make(map[string]flags.FlagSet)
	fs["recent"] = &flags.BoolFlag{Name: "recent", Usage: T("Dump recent logs instead of tailing")}

	return command_registry.CommandMetadata{
		Name:        "logs",
		Description: T("Tail or show recent logs for an app"),
		Usage:       T("CF_NAME logs APP_NAME"),
		Flags:       fs,
	}
}

func (cmd *Logs) Requirements(requirementsFactory requirements.Factory, fc flags.FlagContext) (reqs []requirements.Requirement, err error) {
	if len(fc.Args()) != 1 {
		cmd.ui.Failed(T("Incorrect Usage. Requires an argument\n\n") + command_registry.Commands.CommandUsage("logs"))
	}

	cmd.appReq = requirementsFactory.NewApplicationRequirement(fc.Args()[0])

	reqs = []requirements.Requirement{
		requirementsFactory.NewLoginRequirement(),
		requirementsFactory.NewTargetedSpaceRequirement(),
		cmd.appReq,
	}

	return
}

func (cmd *Logs) SetDependency(deps command_registry.Dependency, pluginCall bool) command_registry.Command {
	cmd.ui = deps.Ui
	cmd.config = deps.Config
	cmd.logsRepo = deps.RepoLocator.GetLogsRepository()
	return cmd
}

func (cmd *Logs) Execute(c flags.FlagContext) {
	app := cmd.appReq.GetApplication()

	if c.Bool("recent") {
		cmd.recentLogsFor(app)
	} else {
		cmd.tailLogsFor(app)
	}
}

func (cmd *Logs) recentLogsFor(app models.Application) {
	cmd.ui.Say(T("Connected, dumping recent logs for app {{.AppName}} in org {{.OrgName}} / space {{.SpaceName}} as {{.Username}}...\n",
		map[string]interface{}{
			"AppName":   terminal.EntityNameColor(app.Name),
			"OrgName":   terminal.EntityNameColor(cmd.config.OrganizationFields().Name),
			"SpaceName": terminal.EntityNameColor(cmd.config.SpaceFields().Name),
			"Username":  terminal.EntityNameColor(cmd.config.Username())}))

	messages, err := cmd.logsRepo.RecentLogsFor(app.Guid)
	if err != nil {
		cmd.handleError(err)
	}

	for _, msg := range messages {
		cmd.ui.Say("%s", msg.ToLog(time.Local))
	}
}

func (cmd *Logs) tailLogsFor(app models.Application) {
	onConnect := func() {
		cmd.ui.Say(T("Connected, tailing logs for app {{.AppName}} in org {{.OrgName}} / space {{.SpaceName}} as {{.Username}}...\n",
			map[string]interface{}{
				"AppName":   terminal.EntityNameColor(app.Name),
				"OrgName":   terminal.EntityNameColor(cmd.config.OrganizationFields().Name),
				"SpaceName": terminal.EntityNameColor(cmd.config.SpaceFields().Name),
				"Username":  terminal.EntityNameColor(cmd.config.Username())}))
	}

	c, err := cmd.logsRepo.TailLogsFor(app.Guid, onConnect)
	if err != nil {
		cmd.handleError(err)
	}

	for msg := range c {
		cmd.ui.Say("%s", msg.ToLog(time.Local))
	}
}

func (cmd *Logs) handleError(err error) {
	switch err.(type) {
	case nil:
	case *errors.InvalidSSLCert:
		cmd.ui.Failed(err.Error() + T("\nTIP: use 'cf login -a API --skip-ssl-validation' or 'cf api API --skip-ssl-validation' to suppress this error"))
	default:
		cmd.ui.Failed(err.Error())
	}
}
