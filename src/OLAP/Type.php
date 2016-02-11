<?php

namespace OLAP;


class Type extends Model {

    public function __construct($options) {

        $this->name = is_string($options) ? $options : (empty($options['name']) ? 'text' : $options['name']);
        $this->options = is_array($options) ? array_diff_key($options, array_flip(['name'])) : [];
    }

    public function getCreation() {

        return $this->getOption('create');
    }

    public function getUsing() {

        return $this->getOption('using');
    }
}