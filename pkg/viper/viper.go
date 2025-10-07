package viper

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// ConfigFlagName is the flag for config
const ConfigFlagName = "config"

var cfgFile string

func init() {
	pflag.StringVarP(&cfgFile, ConfigFlagName, "c", cfgFile, "Read configuration from specified `FILE`, "+
		"support JSON, YAML formats.")
}

// LoadConfig ...
func LoadConfig(conf interface{}) error {
	viper.SetConfigName("config") // name of config file (without extension)
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		configDir := filepath.Dir(cfgFile)
		if configDir != "." {
			viper.AddConfigPath(configDir)
		}
	}

	viper.AddConfigPath("conf")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME")

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	fmt.Println("Using config file:", viper.ConfigFileUsed())

	return viper.Unmarshal(conf, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToTimeHookFunc(time.RFC3339),
		)))
}
