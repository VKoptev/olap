<?php

namespace OLAP;


class Type {

    private $type = '';
    private $options = [];

    public function __construct($options) {

        $this->type = is_string($options) ? $options : (empty($options['name']) ? 'text' : $options['name']);
        $this->options = is_array($options) ? array_diff_key($options, array_flip(['name'])) : [];
    }

    public function getType() {

        return $this->type;
    }

    public function getCreation() {

        return $this->getOption('create');
    }

    public function getUsing() {

        return $this->getOption('using');
    }

    protected function getOption($name) {

        return empty($this->options[$name]) ? '' : $this->options[$name];
    }
}