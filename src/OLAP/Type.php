<?php

namespace OLAP;


class Type {

    private $type = '';
    private $create = '';
    private $using = '';

    public function __construct($options) {

        $this->type = is_string($options) ? $options : (empty($options['name']) ? 'text' : $options['name']);
        foreach (['using', 'create'] as $field) {
            $this->$field = empty($options[$field]) ? '' : $options[$field];
        }
    }

    public function getType() {

        return $this->type;
    }

    public function getCreation() {

        return $this->create;
    }

    public function getUsing() {

        return $this->using;
    }
}